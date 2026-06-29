# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for CREATE TABLE <t> (PRIMARY KEY (...)) AS SELECT ... FROM <src>
#
# CTAS is an alias for a two-step process: create the target table (columns and
# types inferred from the SELECT result, primary key as specified) and then
# INSERT INTO <t> SELECT ... It is not atomic.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest, SyntaxException
from .util import unique_name, new_test_table, new_cql


@pytest.fixture(scope="module")
def src_table(cql, test_keyspace):
    schema = 'p int, c int, v text, w int, PRIMARY KEY (p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for p in range(3):
            for c in range(2):
                cql.execute(f"INSERT INTO {table}(p,c,v,w) VALUES ({p}, {c}, 'v{p}{c}', {p*10+c})")
        yield table


def _rows(cql, table, cols="p, c, v, w"):
    return sorted(list(cql.execute(f"SELECT {cols} FROM {table}")))


# --- Basic full copy: same shape, same primary key -------------------------

def test_ctas_full_copy(cql, test_keyspace, src_table):
    dst = test_keyspace + "." + unique_name()
    try:
        cql.execute(f"CREATE TABLE {dst} (PRIMARY KEY (p, c)) AS SELECT * FROM {src_table}")
        assert _rows(cql, dst) == _rows(cql, src_table)
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {dst}")


# --- Projection: only some columns, PK among them --------------------------

def test_ctas_projection(cql, test_keyspace, src_table):
    dst = test_keyspace + "." + unique_name()
    try:
        cql.execute(f"CREATE TABLE {dst} (PRIMARY KEY (p, c)) AS SELECT p, c, w FROM {src_table}")
        # Target has exactly p, c, w.
        assert _rows(cql, dst, "p, c, w") == _rows(cql, src_table, "p, c, w")
        # v should not exist on the target.
        with pytest.raises(InvalidRequest, match="[Uu]nrecognized|[Uu]ndefined|[Uu]nknown"):
            cql.execute(f"SELECT v FROM {dst}")
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {dst}")


# --- A different primary key than the source -------------------------------

def test_ctas_repartition(cql, test_keyspace, src_table):
    # Choose w as the partition key (it is unique per row here) and p,c as
    # clustering, demonstrating CTAS can repartition the copied data.
    dst = test_keyspace + "." + unique_name()
    try:
        cql.execute(f"CREATE TABLE {dst} (PRIMARY KEY (w, p, c)) AS SELECT * FROM {src_table}")
        assert _rows(cql, dst) == _rows(cql, src_table)
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {dst}")


# --- IF NOT EXISTS is a no-op (no copy) when the table already exists -------

def test_ctas_if_not_exists_is_noop(cql, test_keyspace, src_table):
    with new_test_table(cql, test_keyspace, 'p int, c int, v text, w int, PRIMARY KEY (p, c)') as dst:
        # Table exists and is empty; IF NOT EXISTS CTAS must not copy into it.
        cql.execute(f"CREATE TABLE IF NOT EXISTS {dst} (PRIMARY KEY (p, c)) AS SELECT * FROM {src_table}")
        assert list(cql.execute(f"SELECT count(*) FROM {dst}")) == [(0,)]


# --- Validation: every primary-key column must be selected -----------------

def test_ctas_pk_column_not_selected_is_rejected(cql, test_keyspace, src_table):
    dst = test_keyspace + "." + unique_name()
    # 'c' is not in the projection, so it cannot be used as a key column.
    with pytest.raises(InvalidRequest, match="primary-key column 'c'"):
        cql.execute(f"CREATE TABLE {dst} (PRIMARY KEY (p, c)) AS SELECT p, w FROM {src_table}")


# --- Unqualified names under USE keyspace ----------------------------------

def test_ctas_unqualified_with_use(cql, test_keyspace, src_table):
    src_unqualified = src_table.split('.')[1]
    dst_unqualified = unique_name()
    try:
        with new_cql(cql) as ncql:
            ncql.execute(f"USE {test_keyspace}")
            ncql.execute(f"CREATE TABLE {dst_unqualified} (PRIMARY KEY (p, c)) AS SELECT * FROM {src_unqualified}")
        dst = test_keyspace + "." + dst_unqualified
        assert _rows(cql, dst) == _rows(cql, src_table)
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {test_keyspace}.{dst_unqualified}")
