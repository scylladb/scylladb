# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for prepared statements whose underlying schema changes after they
# were prepared. Ported from scylla-dtest's prepared_statements_test.py.
#############################################################################

from cassandra import InvalidRequest

from .util import new_test_keyspace, new_cql

KS_OPTS = "WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }"


# Prepared statements using dropped indexes should be handled correctly:
# executing one after its index was dropped may fail with InvalidRequest,
# or succeed and still return the right rows, but must not fail in any
# other way.
def test_dropped_index(cql):
    num_rows = 10
    with new_test_keyspace(cql, KS_OPTS) as ks:
        cql.execute(f"CREATE TABLE {ks}.mytable (a int PRIMARY KEY, b int)")
        cql.execute(f"CREATE INDEX bindex ON {ks}.mytable(b)")

        insert_statement = cql.prepare(f"INSERT INTO {ks}.mytable (a, b) VALUES (?, ?)")
        for i in range(num_rows):
            cql.execute(insert_statement, (i, 0))

        query_statement = cql.prepare(f"SELECT * FROM {ks}.mytable WHERE b=?")
        assert len(list(cql.execute(query_statement, (0,)))) == num_rows

        cql.execute(f"DROP INDEX {ks}.bindex")

        try:
            rows = list(cql.execute(query_statement, (0,)))
        except InvalidRequest:
            return
        assert len(rows) == num_rows


# A prepared "SELECT *" must return the table's new columns after an ALTER
# TABLE, on every connection that prepared it -- see CASSANDRA-10786 and
# https://datastax-oss.atlassian.net/browse/JAVA-1196.
def test_prepared_select_star_on_schema_change(cql):
    with new_test_keyspace(cql, KS_OPTS) as ks, new_cql(cql) as cql2:
        cql.execute(f"CREATE TABLE {ks}.mytable (a int PRIMARY KEY, b int)")

        # insert data
        insert_statement = cql.prepare(f"INSERT INTO {ks}.mytable (a, b) VALUES (?, ?)")
        num_rows = 10
        for i in range(num_rows):
            cql.execute(insert_statement, (i, 0))

        # check rows, and save prepared queries
        query_statements = []
        for session in [cql, cql2]:
            query_statement = session.prepare(f"SELECT * FROM {ks}.mytable")
            rows = list(session.execute(query_statement))
            assert num_rows == len(rows)
            for row in rows:
                assert hasattr(row, "b"), "row missing b column"
            query_statements.append((session, query_statement))

        # alter the table
        cql.execute(f"ALTER TABLE {ks}.mytable ADD c int")
        cql.execute(f"ALTER TABLE {ks}.mytable DROP b")

        # run the prepared queries again to check they got the update of the table
        for session, query_statement in query_statements:
            rows = list(session.execute(query_statement))
            assert num_rows == len(rows), "unexpected row count after schema change"
            for row in rows:
                assert hasattr(row, "c"), "row missing c column"
                assert not hasattr(row, "b"), "row shouldn't have b column"
