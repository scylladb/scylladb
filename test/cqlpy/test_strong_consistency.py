# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for strongly consistent tables, i.e., tables in a keyspace created
# with consistency = 'global'. Only tests which are happy with a single node
# and plain CQL belong here - the ones which need raft leadership, the node
# lifecycle or coordinated error injection live in test/cluster instead.
#############################################################################

import collections
import re
import time

import pytest
from cassandra.cluster import ConsistencyLevel, NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import BatchStatement, BatchType, SimpleStatement

from . import nodetool
from .test_service_levels import new_service_level
from .util import ScyllaMetrics, config_value_context, new_materialized_view, new_session, new_test_table, new_user, unique_name


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
        # A timestamp on an individual batch item, rejected when the
        # inner statements are prepared. Whole-batch timestamps are
        # covered by test_batch_attributes_on_sc_table.
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"""
                BEGIN BATCH
                INSERT INTO {table} (pk, v) VALUES (0, 13) USING TIMESTAMP 23;
                INSERT INTO {table} (pk, v) VALUES (0, 14);
                APPLY BATCH
            """)
        # A native protocol batch refers to statements prepared earlier, so
        # the refusal has to land on PREPARE, before there is a batch at all.
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?) USING TIMESTAMP ?")


def test_protocol_timestamp_ignored_on_sc_table(cql, sc_keyspace, test_keyspace):
    """
    The CQL USING TIMESTAMP clause is rejected on a strongly consistent table,
    but the protocol level timestamp - which every driver with client side
    timestamps on, python's among them, puts on every request - is silently
    ignored instead: the raft leader assigns the timestamp and never looks at
    what the client sent. Rejecting it would refuse every statement from such
    a driver, so ignoring is the deliberate choice, and it means the same
    client code orders writes differently on the two kinds of table.

    The eventually consistent half is not decoration, it is the control: it is
    what shows the client timestamp is still being sent at all. Were the driver
    to stop consulting the generator, both writes would get ordinary timestamps
    and that half would fail, instead of the strongly consistent half passing
    for the wrong reason.

    A last write, with a client timestamp far above the row, is the sanity
    check: honoured or ignored, it is the newest write, so it has to land on
    both tables. It is what shows a write sent with a custom client timestamp
    is applied at all, which the shadowed write on the eventually consistent
    table cannot show.
    """
    def insert_with_client_timestamp(table, v, timestamp):
        # The driver has no per-statement client timestamp, so the generator
        # on the cluster is the only lever - and that cluster is shared with
        # every other test in the run, hence the restore.
        generator = cql.cluster.timestamp_generator
        cql.cluster.timestamp_generator = lambda: timestamp
        try:
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, {v})")
        finally:
            cql.cluster.timestamp_generator = generator

    # A day ahead, in microseconds: far above the row, yet well within the 3
    # days restrict_future_timestamp tolerates.
    far_above = int((time.time() + 24 * 60 * 60) * 1_000_000)
    for keyspace, honours_client_timestamp in [(sc_keyspace, False), (test_keyspace, True)]:
        kind = "eventually" if honours_client_timestamp else "strongly"
        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, v int") as table:
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 1)")
            insert_with_client_timestamp(table, 2, 1000)  # 1970, far below the row
            # Honoured, the write is shadowed by the older row; ignored, it wins.
            expected = 1 if honours_client_timestamp else 2
            assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == expected, \
                f"{kind} consistent table: expected v={expected}"

            insert_with_client_timestamp(table, 3, far_above)
            assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 3, \
                f"{kind} consistent table: expected the write far above the row to land"


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
    - statement touching multiple partition keys.
    """

    def _prepared_batch_type(kind):
        if kind == "logged":
            return BatchType.LOGGED
        if kind == "unlogged":
            return BatchType.UNLOGGED
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
        raise ValueError(f"Unexpected operation: {op}")

    def _make_batch_runner(table_name):
        prepared_statements = {}
        if batch_mode == "prepared":
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

            if kind == "unlogged":
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


# The single statement write path used to hand an empty JSON cache to the code
# building the keys, so an INSERT ... JSON killed the coordinator on the
# uninitialized storage. Reproduces SCYLLADB-3997.
def test_insert_json(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"""INSERT INTO {table} JSON '{{"pk": 1, "v": 2}}'""")
        assert list(cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1")) == [(1, 2)]


# Clustering key columns come from the same cache as the partition key ones.
def test_insert_json_with_clustering_key(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"""INSERT INTO {table} JSON '{{"pk": 1, "ck": 2, "v": 3}}'""")
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 1")) == [(1, 2, 3)]


# A bound JSON term is only known when the statement is executed, which is why
# the cache cannot be prepared any earlier than that.
def test_insert_json_prepared(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        insert = cql.prepare(f"INSERT INTO {table} JSON ?")
        cql.execute(insert, ['{"pk": 1, "v": 2}'])
        assert list(cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1")) == [(1, 2)]


# A batch prepares the cache for each of its statements, which is what pins the
# defect above down to the single statement path.
def test_insert_json_in_batch(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"""
            BEGIN UNLOGGED BATCH
            INSERT INTO {table} JSON '{{"pk": 1, "v": 2}}';
            APPLY BATCH
        """)
        assert list(cql.execute(f"SELECT pk, v FROM {table} WHERE pk = 1")) == [(1, 2)]


def test_counter_table_creation(cql, sc_keyspace):
    """
    Counter tables are rejected in strongly consistent keyspaces: a
    counter update is a delta, and the write path would store it raw
    instead of folding it into the counter value, aborting the node
    when the counter is read back.
    """
    with pytest.raises(InvalidRequest, match="counters are not supported in strongly consistent keyspaces"):
        cql.execute(f"CREATE TABLE {sc_keyspace}.{unique_name()} (pk int PRIMARY KEY, c counter)")


def test_counter_batch_on_sc_table(cql, sc_keyspace):
    """
    Counter batches are rejected on strongly consistent keyspaces.
    The batch type is checked before the statements in it, so the
    batch here carries a regular INSERT. It could not carry a counter
    update anyway: that needs a counter table, and counter tables
    cannot be created in strongly consistent keyspaces (see
    test_counter_table_creation).
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        error_msg = "Counter batches are not supported with strongly consistent tables"
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"""
                BEGIN COUNTER BATCH
                INSERT INTO {table} (pk, v) VALUES (1, 2);
                APPLY BATCH
            """)

        batch = BatchStatement(batch_type=BatchType.COUNTER)
        batch.add(cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)"), (1, 2))
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(batch)


def test_cdc_on_sc_table(cql, sc_keyspace):
    """
    CDC is rejected on tables in strongly consistent keyspaces, at
    CREATE TABLE and with ALTER TABLE. The CDC log would never be
    populated: log rows are generated on the coordinator write path,
    which strongly consistent writes bypass.
    """
    error_msg = "CDC is not supported in strongly consistent keyspaces"
    with pytest.raises(InvalidRequest, match=error_msg):
        cql.execute(f"CREATE TABLE {sc_keyspace}.{unique_name()} (pk int PRIMARY KEY, v int) WITH cdc = {{'enabled': true}}")

    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"ALTER TABLE {table} WITH cdc = {{'enabled': true}}")


def test_per_partition_rate_limit_on_sc_table(cql, sc_keyspace):
    """
    per_partition_rate_limit is rejected for tables in strongly
    consistent keyspaces. Rate limits are enforced on the coordinator
    read and write paths, which strongly consistent queries bypass.
    """
    rate_limit_msg = "Per-partition rate limit is not supported in strongly consistent keyspaces"
    with pytest.raises(ConfigurationException, match=rate_limit_msg):
        cql.execute(f"CREATE TABLE {sc_keyspace}.{unique_name()} (pk int PRIMARY KEY, v int)"
                    " WITH per_partition_rate_limit = {'max_writes_per_second': 100}")

    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        with pytest.raises(ConfigurationException, match=rate_limit_msg):
            cql.execute(f"ALTER TABLE {table} WITH per_partition_rate_limit = {{'max_reads_per_second': 100}}")


def test_views_and_indexes_on_sc_table(cql, sc_keyspace):
    """
    Materialized views and secondary indexes are rejected on tables in
    strongly consistent keyspaces. They would never be updated: view
    updates are generated on the coordinator write path, which strongly
    consistent writes bypass.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        error_msg = "not supported on tables in strongly consistent keyspaces"
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"CREATE MATERIALIZED VIEW {table}_mv AS"
                        f" SELECT * FROM {table} WHERE v IS NOT NULL AND pk IS NOT NULL"
                        f" PRIMARY KEY (v, pk)")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"CREATE INDEX ON {table} (v)")


def test_lwt_on_sc_table(cql, sc_keyspace):
    """
    Conditional statements (LWT) are rejected on strongly consistent
    tables, standalone and inside a batch. The strongly consistent
    execution path never evaluates conditions, so accepting them would
    silently execute the write unconditionally. INSERT JSON is covered
    separately: its IF NOT EXISTS is not tracked as a condition in the
    prepared statement.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 1)")

        error_msg = "Strongly consistent updates don't support conditions"
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 999) IF NOT EXISTS")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"UPDATE {table} SET v = 7 WHERE pk = 1 IF v = 12345")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"UPDATE {table} SET v = 7 WHERE pk = 1 IF EXISTS")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"DELETE FROM {table} WHERE pk = 1 IF EXISTS")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"""INSERT INTO {table} JSON '{{"pk": 1, "v": 999}}' IF NOT EXISTS""")
        with pytest.raises(InvalidRequest, match=error_msg):
            cql.execute(f"""
                BEGIN BATCH
                UPDATE {table} SET v = 8 WHERE pk = 1 IF v = 12345;
                APPLY BATCH
            """)

        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 1


def test_reject_data_prefetch_on_sc_table(cql, sc_keyspace):
    """
    The strongly consistent write path does not support read-before-write
    yet: the coordinator builds the mutation from the statement alone, with
    no prefetched row, so a statement whose operations need the current row
    is refused when it is prepared, rather than computed from nothing.

    The operations which need the row are those reporting requires_read():
    three list operations - removing by value, setting by index, deleting by
    index - and setting a regular column from an expression over a non-key
    column, like SET v = v + 1. The last one is refused on every table unless
    it carries a condition, which is what makes it atomic, so it reaches the
    strongly consistent refusal only with IF EXISTS; the prefetch check runs
    before the refusal of conditions, which is why the message is still the
    prefetch one.

    An append needs no read and goes through, which is what separates a
    refusal about prefetching from a refusal about collections.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int, l list<int>") as table:
        cql.execute(f"INSERT INTO {table} (pk, v, l) VALUES (1, 1, [10, 20, 30])")
        error_msg = "Strongly consistent updates don't support data prefetch"
        for statement in [f"UPDATE {table} SET l = l - [20] WHERE pk = 1",
                          f"UPDATE {table} SET l[0] = 99 WHERE pk = 1",
                          f"DELETE l[0] FROM {table} WHERE pk = 1",
                          f"UPDATE {table} SET v = v + 1 WHERE pk = 1 IF EXISTS"]:
            with pytest.raises(InvalidRequest, match=error_msg):
                cql.execute(statement)

        cql.execute(f"UPDATE {table} SET l = l + [40] WHERE pk = 1")
        assert list(cql.execute(f"SELECT v, l FROM {table} WHERE pk = 1")) == [(1, [10, 20, 30, 40])]


def test_batch_attributes_on_sc_table(cql, sc_keyspace):
    """
    Batch-level USING TTL and USING TIMESTAMP are rejected on strongly
    consistent batches. The strongly consistent batch reads its USING
    attributes only to compute the timeout, so both would be parsed
    and then silently ignored.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        with pytest.raises(InvalidRequest, match="Global TTL on the BATCH statement is not supported"):
            cql.execute(f"""
                BEGIN BATCH USING TTL 100
                INSERT INTO {table} (pk, v) VALUES (1, 2);
                INSERT INTO {table} (pk, v) VALUES (1, 3);
                APPLY BATCH
            """)

        with pytest.raises(InvalidRequest, match="Strongly consistent queries don't support user-provided timestamps"):
            cql.execute(f"""
                BEGIN BATCH USING TIMESTAMP 42
                INSERT INTO {table} (pk, v) VALUES (2, 2);
                INSERT INTO {table} (pk, v) VALUES (2, 3);
                APPLY BATCH
            """)


def test_batch_consistency_level_on_sc_table(cql, sc_keyspace):
    """
    A strongly consistent batch requires QUORUM or LOCAL_QUORUM, like
    a standalone strongly consistent write.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cl_error = "Strongly consistent writes must use QUORUM/LOCAL_QUORUM"
        with pytest.raises(InvalidRequest, match=cl_error):
            cql.execute(SimpleStatement(
                f"INSERT INTO {table} (pk, v) VALUES (1, 2)",
                consistency_level=ConsistencyLevel.ONE))

        with pytest.raises(InvalidRequest, match=cl_error):
            cql.execute(SimpleStatement(
                f"BEGIN BATCH INSERT INTO {table} (pk, v) VALUES (1, 2); APPLY BATCH",
                consistency_level=ConsistencyLevel.ONE))

        cql.execute(SimpleStatement(
            f"BEGIN BATCH INSERT INTO {table} (pk, v) VALUES (1, 2); APPLY BATCH",
            consistency_level=ConsistencyLevel.QUORUM))
        assert cql.execute(f"SELECT v FROM {table} WHERE pk = 1").one().v == 2


def test_read_consistency_level_on_sc_table(cql, sc_keyspace):
    """
    A strongly consistent read accepts QUORUM/LOCAL_QUORUM, which it serves as
    a linearizable read, and ONE/LOCAL_ONE, which it serves off a single
    replica, and rejects every other consistency level - a wider set than the
    write path takes, which is QUORUM/LOCAL_QUORUM only. The two lists below
    are all the levels there are. The check precedes the generic validation of
    a read level, so even a write-only level like ANY is refused with this
    message rather than with the usual complaint about ANY not being a read
    level.

    Which of the two paths a level actually takes is not visible here: on a
    single node with RF=1 a linearizable read and one served off a single
    replica return the same thing. This pins the accept/reject split only; the
    difference in behaviour belongs in test/cluster.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 2)")
        select = f"SELECT v FROM {table} WHERE pk = 1"

        accepted = [ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM,
                    ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE]
        refused = [ConsistencyLevel.ANY, ConsistencyLevel.TWO, ConsistencyLevel.THREE,
                   ConsistencyLevel.ALL, ConsistencyLevel.EACH_QUORUM,
                   ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL]
        # The claim of the test is that these are all of them.
        assert set(accepted) | set(refused) == set(ConsistencyLevel.name_to_value.values())

        for cl in accepted:
            assert cql.execute(SimpleStatement(select, consistency_level=cl)).one().v == 2

        cl_error = "Strongly consistent reads must use QUORUM/LOCAL_QUORUM or ONE/LOCAL_ONE"
        for cl in refused:
            with pytest.raises(InvalidRequest, match=cl_error):
                cql.execute(SimpleStatement(select, consistency_level=cl))


def test_mixed_keyspace_batch_on_sc_table(cql, sc_keyspace, test_keyspace):
    """
    A CQL text batch that mixes statements on strongly consistent and
    eventually consistent keyspaces is rejected, in both statement
    orders, like on the native protocol batch path. Such a batch
    cannot be executed: strongly consistent writes go through the
    tablet raft groups and eventually consistent writes do not.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as sc_table:
        with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as ec_table:
            error_msg = "Cannot mix strongly consistent and eventually consistent statements in a batch"
            with pytest.raises(InvalidRequest, match=error_msg):
                cql.execute(f"""
                    BEGIN BATCH
                    INSERT INTO {ec_table} (pk, v) VALUES (1, 1);
                    INSERT INTO {sc_table} (pk, v) VALUES (1, 2);
                    APPLY BATCH
                """)

            with pytest.raises(InvalidRequest, match=error_msg):
                cql.execute(f"""
                    BEGIN BATCH
                    INSERT INTO {sc_table} (pk, v) VALUES (1, 2);
                    INSERT INTO {ec_table} (pk, v) VALUES (1, 1);
                    APPLY BATCH
                """)

            # A mixed batch is classified in two independent places - here in
            # batch_statement::prepare() for the textual form, and in
            # transport/server.cc for the native protocol one - and has already
            # been let through once, when the textual path asked only about the
            # keyspace of the first statement. The eventually consistent half
            # is the telling one: it is written through the ordinary path, so
            # it would leak whatever the raft group did.
            assert list(cql.execute(f"SELECT pk, v FROM {sc_table} WHERE pk = 1")) == []
            assert list(cql.execute(f"SELECT pk, v FROM {ec_table} WHERE pk = 1")) == []


def test_group_by_on_sc_table(cql, sc_keyspace):
    """
    GROUP BY is rejected on strongly consistent SELECTs. The strongly
    consistent read path builds its result set without the grouping
    indices, so an aggregate over GROUP BY would return one global row
    instead of one row per group.
    """
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for ck in (1, 2, 3):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, {ck})")

        with pytest.raises(InvalidRequest, match="Strongly consistent queries don't support GROUP BY"):
            cql.execute(f"SELECT ck, count(v) FROM {table} WHERE pk = 1 GROUP BY pk, ck")


def test_prune_materialized_view_on_sc_table(cql, sc_keyspace, test_keyspace):
    """
    PRUNE MATERIALIZED VIEW is rejected on strongly consistent tables.
    It would otherwise be dispatched as a plain strongly consistent
    read, a silent no-op. The eventually consistent behavior serves as
    contrast: PRUNE is rejected on a plain table and returns no rows
    from a view.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 2)")

        with pytest.raises(InvalidRequest, match="PRUNE MATERIALIZED VIEW is not supported on strongly consistent tables"):
            cql.execute(f"PRUNE MATERIALIZED VIEW {table} WHERE pk = 1")

    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as ec_table:
        cql.execute(f"INSERT INTO {ec_table} (pk, v) VALUES (1, 2)")

        with pytest.raises(InvalidRequest, match="Ghost rows can only be deleted from materialized views"):
            cql.execute(f"PRUNE MATERIALIZED VIEW {ec_table} WHERE pk = 1")

        with new_materialized_view(cql, ec_table, '*', 'v, pk', 'v is not null and pk is not null') as mv:
            assert list(cql.execute(f"PRUNE MATERIALIZED VIEW {mv} WHERE v = 2")) == []


def test_null_primary_key_on_sc_table(cql, sc_keyspace):
    """
    Null primary key values are rejected on strongly consistent writes,
    as on eventually consistent ones. Without the check a null clustering
    key value was accepted and the write changed nothing.
    """
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
        with pytest.raises(InvalidRequest, match="Invalid null value in condition for column ck"):
            cql.execute(insert, [1, None, 1])
        with pytest.raises(InvalidRequest, match="Invalid null value in condition for column pk"):
            cql.execute(insert, [None, 1, 1])

        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.add(insert, [1, None, 1])
        with pytest.raises(InvalidRequest, match="Invalid null value in condition for column ck"):
            cql.execute(batch)

        assert list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1")) == []


def test_oversized_write_on_sc_table(cql, sc_keyspace):
    """
    A write whose raft command exceeds the command size limit of the
    group is rejected with a clear error, not the raft error surfacing as
    a server error. The rejection leaves the table writable.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v blob") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        cql.execute(insert, [1, b'x' * 1024])
        with pytest.raises(InvalidRequest, match="Strongly consistent write of .* bytes exceeds the limit of .* bytes") as e:
            cql.execute(insert, [2, b'x' * (200 * 1024)])
        m = re.search(r"write of (\d+) bytes exceeds the limit of (\d+) bytes", str(e.value))
        size, limit = int(m.group(1)), int(m.group(2))
        assert size >= 200 * 1024 > limit

        # Close to the limit, so the test also fails if the limit shrinks.
        cql.execute(insert, [3, b'y' * (90 * 1024)])
        assert [r.v for r in cql.execute(f"SELECT v FROM {table} WHERE pk = 1")] == [b'x' * 1024]
        assert list(cql.execute(f"SELECT v FROM {table} WHERE pk = 2")) == []
        assert [r.v for r in cql.execute(f"SELECT v FROM {table} WHERE pk = 3")] == [b'y' * (90 * 1024)]


def test_mutation_fragments_on_sc_table(cql, sc_keyspace):
    """
    SELECT FROM MUTATION_FRAGMENTS() dumps a strongly consistent table
    like any other table. Strongly consistent writes are applied into
    the table's memtable, a flush moves them to an sstable, and a read
    populates the row cache. The dump reads the local replica only, so
    it is issued at ONE.
    """
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for ck in range(3):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, {ck})")
        # A write returns once committed. The linearizable read waits
        # for it to be applied, which is what the dump reads.
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))) == 3

        dump = SimpleStatement(f"SELECT mutation_source, mutation_fragment_kind, ck FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1",
                               consistency_level=ConsistencyLevel.ONE)

        def fragments(source):
            return [(r.mutation_fragment_kind, r.ck) for r in cql.execute(dump) if r.mutation_source.startswith(source)]

        expected = [('partition start', None), ('clustering row', 0), ('clustering row', 1), ('clustering row', 2), ('partition end', None)]
        assert fragments('memtable:') == expected
        assert fragments('sstable:') == []

        nodetool.flush(cql, table)
        assert fragments('memtable:') == []
        assert fragments('sstable:') == expected

        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))) == 3
        assert fragments('row-cache') == expected


def test_mutation_fragments_paging_on_sc_table(cql, sc_keyspace):
    """
    A paged dump of a strongly consistent table returns the same
    fragments as the unpaged one, whatever the page size.
    """
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for ck in range(23):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, {ck})")
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))) == 23

        query = f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"
        expected = list(cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE)))
        # partition start, 23 rows, partition end
        assert len(expected) == 25

        for page_size in (1, 7, 10, 24, 25):
            statement = SimpleStatement(query, fetch_size=page_size, consistency_level=ConsistencyLevel.ONE)
            assert list(cql.execute(statement)) == expected, f"fetch_size={page_size}"


def test_mutation_fragments_group_by_on_sc_table(cql, sc_keyspace):
    """
    GROUP BY is rejected on strongly consistent SELECTs, but a dump of a
    strongly consistent table groups like on any other table. COUNT()
    per mutation source matches the ungrouped dump.
    """
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        for ck in range(3):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, {ck})")
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))) == 3
        nodetool.flush(cql, table)
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 3, 3)")
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))) == 4

        def dump(select, group_by=""):
            query = f"SELECT {select} FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1 {group_by}"
            return cql.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ONE))

        expected = sorted(collections.Counter(r.mutation_source for r in dump("mutation_source")).items())
        assert len(expected) >= 2
        rows = dump("mutation_source, COUNT(*)", "GROUP BY pk, mutation_source")
        assert sorted((r.mutation_source, r.count) for r in rows) == expected


def test_mutation_fragments_consistency_level_on_sc_table(cql, sc_keyspace):
    """
    A dump of a strongly consistent table is accepted only at ONE and
    LOCAL_ONE. It reads the coordinator only, with no read barrier. A
    regular read at ONE also reads one replica with no barrier, but is
    redirected when the coordinator is not a replica. QUORUM and
    LOCAL_QUORUM promise a linearizable read the dump cannot give.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (1, 2)")
        assert len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 1"))) == 1

        query = f"SELECT * FROM MUTATION_FRAGMENTS({table}) WHERE pk = 1"
        for cl in (ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE):
            # partition start, clustering row, partition end
            assert len(list(cql.execute(SimpleStatement(query, consistency_level=cl)))) == 3

        error_msg = "MUTATION_FRAGMENTS\\(\\) on strongly consistent tables must use ONE/LOCAL_ONE consistency level"
        for cl in (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ALL):
            with pytest.raises(InvalidRequest, match=error_msg):
                cql.execute(SimpleStatement(query, consistency_level=cl))


def test_coordinator_metrics_per_scheduling_group(cql, sc_keyspace):
    """
    Verify that the coordinator metrics are split by scheduling group: the requests of
    a role with its own service level are counted under that service level's
    scheduling group, and a request of another role is not.

    No other test runs in that scheduling group: the service level is attached only to
    this test's role. But the group may be reused from a dropped service level and keep
    its counts, so the test compares counts before and after its requests.
    """
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, c int") as table, \
            new_user(cql, with_superuser_privileges=True) as role, \
            new_service_level(cql, role=role) as sl, \
            new_session(cql, role) as session:
        scheduling_group = f"sl:{sl}"

        def write_count():
            return ScyllaMetrics.query(cql).get('scylla_strong_consistency_coordinator_write_latency_count',
                                                {'scheduling_group_name': scheduling_group}) or 0

        def read_count():
            return ScyllaMetrics.query(cql).get('scylla_strong_consistency_coordinator_read_latency_count',
                                                {'scheduling_group_name': scheduling_group, 'read_type': 'linearizable'}) or 0

        writes_before = write_count()
        reads_before = read_count()
        session.execute(f"INSERT INTO {table} (pk, c) VALUES (1, 1)")
        session.execute(f"INSERT INTO {table} (pk, c) VALUES (2, 2)")
        session.execute(f"SELECT * FROM {table} WHERE pk = 1")
        assert write_count() - writes_before == 2
        assert read_count() - reads_before == 1

        # A write of another role is not counted in this scheduling group.
        writes_before = write_count()
        cql.execute(f"INSERT INTO {table} (pk, c) VALUES (3, 3)")
        assert write_count() - writes_before == 0


#############################################################################
# Tests for the shape of single-partition reads from strongly consistent
# tables - clustering slices, limits, ordering, DISTINCT, static columns,
# aggregates, filtering and paging.
#
# A strongly consistent SELECT does not go through storage_proxy and the
# query pagers. It builds its read_command by hand in
# cql3/statements/strong_consistency/select_statement.cc and runs it as one
# local query. So everything the regular path gets from the pagers - paging
# itself, the internal paging of aggregates and of filtering queries - has to
# be checked separately here. The slice itself is built by code shared with
# the regular path, so the cases below aim at what the strongly consistent
# path sets on its own: the row and partition limits, the result size limit,
# and the absence of paging.
#
# The expected result of every query is what the same query returns from an
# eventually consistent table holding the same data. The read path of such a
# table is the reference implementation of CQL semantics, so the tests don't
# repeat them.
#############################################################################

SCHEMA = "pk int, ck int, s int static, v int, PRIMARY KEY (pk, ck)"

# pk=1: a partition with a static row and 20 clustering rows, with v repeating
#       so that filtering on it and aggregating over it pick several rows.
# pk=2: a partition with only a static row.
ROWS = 20


def fill(cql, table):
    cql.execute(f"INSERT INTO {table} (pk, s) VALUES (1, 7)")
    insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (1, ?, ?)")
    execute_concurrent_with_args(cql, insert, [(ck, ck % 5) for ck in range(ROWS)], raise_on_first_error=True)
    cql.execute(f"INSERT INTO {table} (pk, s) VALUES (2, 8)")


@pytest.fixture(scope="module")
def tables(cql, sc_keyspace, test_keyspace):
    with new_test_table(cql, sc_keyspace, SCHEMA) as sc_table:
        with new_test_table(cql, test_keyspace, SCHEMA) as ec_table:
            fill(cql, sc_table)
            fill(cql, ec_table)
            yield sc_table, ec_table


def select(cql, query, table, **kwargs):
    return list(cql.execute(SimpleStatement(query.format(table=table), **kwargs)))


def assert_same_result(cql, tables, query, **kwargs):
    sc_table, ec_table = tables
    expected = select(cql, query, ec_table, **kwargs)
    assert select(cql, query, sc_table, **kwargs) == expected


# Filtering is done by the coordinator after the replica has applied the row
# limit, which is why the regular path pages internally through a filtering
# query rather than passing its LIMIT down to the replica. The strongly
# consistent path passes it down, so the replica returns the first LIMIT rows
# of the partition and the filter drops the ones that don't match.
limit_before_filter = pytest.mark.xfail(strict=True, raises=AssertionError,
    reason="SCYLLADB-5429: strongly consistent read applies LIMIT on the replica, before ALLOW FILTERING drops rows")


@pytest.mark.parametrize("query", [
    # Clustering slices.
    "SELECT * FROM {table} WHERE pk = 1",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 AND ck < 9",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck IN (2, 5, 100)",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 100",
    # The row limit and the per-partition limit the strongly consistent path
    # puts into the read_command itself, with get_inner_loop_limit().
    "SELECT ck, v FROM {table} WHERE pk = 1 LIMIT 4",
    "SELECT ck, v FROM {table} WHERE pk = 1 PER PARTITION LIMIT 4",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 AND ck < 9 LIMIT 2",
    # A reversed query reads with the reversed schema and reverses the
    # clustering bounds, so a slice and a limit have to come out of the other
    # end of the partition.
    "SELECT ck, v FROM {table} WHERE pk = 1 ORDER BY ck DESC LIMIT 4",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 AND ck < 9 ORDER BY ck DESC LIMIT 2",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck IN (2, 5, 100) ORDER BY ck DESC",
    # Static columns and DISTINCT.
    "SELECT * FROM {table} WHERE pk = 2",
    "SELECT s FROM {table} WHERE pk = 1 LIMIT 3",
    "SELECT DISTINCT pk, s FROM {table} WHERE pk = 1",
    # An aggregate reads with no row limit (get_inner_loop_limit() returns
    # max_rows for it), and LIMIT then applies to the single aggregated row.
    "SELECT count(v), min(v), max(v), sum(v), avg(v) FROM {table} WHERE pk = 1",
    "SELECT count(*) FROM {table} WHERE pk = 1 LIMIT 1",
    # Filtering.
    "SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ALLOW FILTERING",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ORDER BY ck DESC ALLOW FILTERING",
    "SELECT count(*) FROM {table} WHERE pk = 1 AND v = 3 ALLOW FILTERING",
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 LIMIT 2 ALLOW FILTERING",
                 marks=limit_before_filter),
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 PER PARTITION LIMIT 2 ALLOW FILTERING",
                 marks=limit_before_filter),
    pytest.param("SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ORDER BY ck DESC LIMIT 2 ALLOW FILTERING",
                 marks=limit_before_filter),
])
def test_select(cql, tables, query):
    assert_same_result(cql, tables, query)


# The strongly consistent path reads the whole result at once, whatever the
# page size the client asked for. However it's served, the rows the client
# ends up with have to be the ones an eventually consistent table returns. A
# page size that doesn't divide the result leaves a partial last page.
@pytest.mark.parametrize("query", [
    "SELECT * FROM {table} WHERE pk = 1",
    "SELECT ck, v FROM {table} WHERE pk = 1 ORDER BY ck DESC",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND ck > 3 LIMIT 7",
    "SELECT ck, v FROM {table} WHERE pk = 1 AND v = 3 ALLOW FILTERING",
])
def test_paging(cql, tables, query):
    assert_same_result(cql, tables, query, fetch_size=3)


# Paged strongly consistent reads are deferred (SCYLLADB-2589), so a result
# has to fit in a single read, bounded like an unpaged regular read by
# max_memory_for_unlimited_query_hard_limit (100 MB by default). 150 rows of
# 10 KB each go over 1 MB - the regular page size - while staying far under
# that default, and each write stays well under the raft command size
# limit. count(v) and max(v) read every cell, like the plain read - on the
# regular path aggregates are paged internally whatever the client's page size.
LARGE_ROWS = 150
LARGE_VALUE = b'x' * 10 * 1024


@pytest.fixture(scope="module")
def large_partition_tables(cql, sc_keyspace, test_keyspace):
    schema = "pk int, ck int, v blob, PRIMARY KEY (pk, ck)"
    with new_test_table(cql, sc_keyspace, schema) as sc_table:
        with new_test_table(cql, test_keyspace, schema) as ec_table:
            for table in (sc_table, ec_table):
                insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (1, ?, ?)")
                execute_concurrent_with_args(cql, insert, [(ck, LARGE_VALUE) for ck in range(LARGE_ROWS)], raise_on_first_error=True)
            yield sc_table, ec_table


@pytest.mark.parametrize("query", [
    "SELECT ck, v FROM {table} WHERE pk = 1",
    "SELECT ck, v FROM {table} WHERE pk = 1 ORDER BY ck DESC",
    "SELECT count(*) FROM {table} WHERE pk = 1",
    "SELECT count(v) FROM {table} WHERE pk = 1",
    "SELECT max(v) FROM {table} WHERE pk = 1",
])
def test_partition_larger_than_result_size_limit(cql, large_partition_tables, query):
    assert_same_result(cql, large_partition_tables, query, fetch_size=10)


# The limit is the configured one: with the hard limit lowered to 512 KB, a
# read of 80 rows (~800 KB) fails, and with the limit restored it succeeds.
@pytest.mark.parametrize("order", ["ASC", "DESC"])
def test_result_size_limit_is_configurable(cql, large_partition_tables, order):
    sc_table, _ = large_partition_tables
    limit = str(512 * 1024)
    query = f"SELECT ck, v FROM {sc_table} WHERE pk = 1 ORDER BY ck {order} LIMIT 80"
    with config_value_context(cql, 'max_memory_for_unlimited_query_soft_limit', limit), \
            config_value_context(cql, 'max_memory_for_unlimited_query_hard_limit', limit):
        with pytest.raises(NoHostAvailable, match=f"exceeds hard limit of {limit}"):
            cql.execute(SimpleStatement(query, fetch_size=10))
    assert len(list(cql.execute(SimpleStatement(query, fetch_size=10)))) == 80


#############################################################################
# Tests for the restrictions a strongly consistent table puts on a statement.
#
# A strongly consistent statement is executed by a single raft group, the one
# owning the tablet of the partition it touches, which is why the coordinator
# rejects anything that does not resolve to exactly one partition, and anything
# mixing a strongly consistent table with an eventually consistent one in a
# single batch.  Each rejection happens on the coordinator before the raft
# group is ever contacted, so a single node and plain CQL are enough.
#
# The rejections themselves live in:
#   cql3/statements/strong_consistency/select_statement.cc       (reads)
#   cql3/statements/strong_consistency/modification_statement.cc (writes)
#   transport/server.cc                                          (mixed batch)
#
# Restrictions which apply to a batch as a whole - a batch mixing keyspaces in
# its textual form, batch level USING TIMESTAMP and USING TTL, consistency
# levels, counters - are tested next to the rest of the batch behaviour above.
#############################################################################

# Raised by both the read and the write path for a statement that does not
# resolve to exactly one partition.
ERR_SINGLE_PARTITION = "Strongly consistent queries can only target a single partition"
# Raised by the native protocol batch path for a batch which is part strongly
# consistent and part eventually consistent.
ERR_CANNOT_MIX = "Cannot mix strongly consistent and eventually consistent statements in a batch"


# Every shape of read which does not resolve to a single partition goes through
# the same check, so one representative of each is enough: a full scan, a scan
# with an aggregate on top, several single-partition ranges, and a token range.
# The aggregate is worth a case of its own because the strongly consistent
# branch of select_statement::prepare() is picked before can_be_mapreduced(),
# so a count(*) never reaches the parallelized aggregation path which would
# have fanned the query out to every shard of every replica.  An
# ALLOW FILTERING query is the same full scan and is not repeated here.
def test_reject_multi_partition_reads(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 10)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (2, 1, 20)")

        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT * FROM {table}")
        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT count(*) FROM {table}")
        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT * FROM {table} WHERE pk IN (1, 2)")
        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT * FROM {table} WHERE token(pk) > 0")

        # A single partition, however the restriction spells it, is fine - an
        # IN with one element resolves to one range like a plain equality does.
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 1")) == [(1, 1, 10)]
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk IN (2)")) == [(2, 1, 20)]


# The write path builds its partition keys and applies the same check, so a
# DELETE spanning two partitions is rejected.  An UPDATE with the same IN
# restriction shares build_partition_keys() and is not repeated here.  The same
# DELETE inside a batch is covered by test_batch; this is the standalone
# statement, which reaches the check through modification_statement rather than
# through the batch.
def test_reject_multi_partition_writes(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 10)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (2, 1, 20)")

        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"DELETE FROM {table} WHERE pk IN (1, 2) AND ck = 1")

        # A single partition, however the restriction spells it: the check
        # counts the keys the statement builds, so a one element IN is as good
        # as an equality - and the delete takes that row and nothing else.
        cql.execute(f"DELETE FROM {table} WHERE pk IN (2) AND ck = 1")
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 2")) == []
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 1")) == [(1, 1, 10)]


# A native protocol batch carries prepared statements which the server has
# already classified, so it can tell a mixed batch apart from a homogeneous one
# and rejects it.  The check counts the strongly consistent statements against
# the batch size, so the order the two halves arrive in does not matter.  The
# textual form of the same batch is rejected elsewhere, by
# batch_statement::prepare(), and is covered by
# test_mixed_keyspace_batch_on_sc_table.
def test_reject_mixed_batch_native_protocol(cql, sc_keyspace, test_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as sc_table, \
         new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as ec_table:
        sc_insert = cql.prepare(f"INSERT INTO {sc_table} (pk, v) VALUES (?, ?)")
        ec_insert = cql.prepare(f"INSERT INTO {ec_table} (pk, v) VALUES (?, ?)")

        for first, second in [(sc_insert, ec_insert), (ec_insert, sc_insert)]:
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.add(first, (1, 10))
            batch.add(second, (1, 20))
            with pytest.raises(InvalidRequest, match=ERR_CANNOT_MIX):
                cql.execute(batch)

        # Neither half of a rejected batch may be applied.
        assert list(cql.execute(f"SELECT pk, v FROM {sc_table} WHERE pk = 1")) == []
        assert list(cql.execute(f"SELECT pk, v FROM {ec_table} WHERE pk = 1")) == []
