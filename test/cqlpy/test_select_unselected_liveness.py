# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# A row (or a partition with a static row) is returned by SELECT if it has
# any live cell, even when that cell's column is not selected. These tests
# check that liveness of unselected columns is still honored.
#############################################################################

import time
import pytest
from cassandra.query import SimpleStatement
from .util import unique_name, unique_key_int, new_test_table, new_materialized_view
from . import nodetool


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int, c int, a int, b int, s set<text>, st int static, PRIMARY KEY (p, c))")
    yield table
    cql.execute("DROP TABLE " + table)


# UPDATE writes no row marker, so the row lives only through the cell it sets.
def test_row_live_only_through_unselected_cell(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} SET b = 1 WHERE p = {p} AND c = 1")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == [(None,)]
    assert list(cql.execute(f"SELECT c, a FROM {table1} WHERE p = {p}")) == [(1, None)]


def test_row_live_only_through_unselected_collection(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} SET s = {{'x', 'y'}} WHERE p = {p} AND c = 1")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == [(None,)]


def test_row_with_deleted_unselected_cell(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} SET b = 1 WHERE p = {p} AND c = 1")
    cql.execute(f"DELETE b FROM {table1} WHERE p = {p} AND c = 1")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == []


def test_row_with_emptied_unselected_collection(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} SET s = {{'x'}} WHERE p = {p} AND c = 1")
    cql.execute(f"UPDATE {table1} SET s = s - {{'x'}} WHERE p = {p} AND c = 1")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == []


def test_unselected_cell_covered_by_row_tombstone(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} USING TIMESTAMP 1 SET b = 1 WHERE p = {p} AND c = 1")
    cql.execute(f"DELETE FROM {table1} USING TIMESTAMP 2 WHERE p = {p} AND c = 1")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == []


def test_partition_live_only_through_unselected_static(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} SET st = 1 WHERE p = {p}")
    assert list(cql.execute(f"SELECT p FROM {table1} WHERE p = {p}")) == [(p,)]
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == [(None,)]


def test_selected_and_unselected_cells(cql, table1):
    p = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (p, c, a, b, s) VALUES ({p}, 1, 10, 20, {{'x'}})")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == [(10,)]
    assert list(cql.execute(f"SELECT b, s FROM {table1} WHERE p = {p}")) == [(20, {'x'})]


# Row alive only through an unselected cell that expires.
def test_row_live_only_through_expiring_unselected_cell(cql, table1):
    p = unique_key_int()
    cql.execute(f"UPDATE {table1} USING TTL 5 SET b = 1 WHERE p = {p} AND c = 1")
    assert list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")) == [(None,)]
    deadline = time.time() + 20
    while list(cql.execute(f"SELECT a FROM {table1} WHERE p = {p}")):
        assert time.time() < deadline
        time.sleep(0.1)


def test_row_live_only_through_unselected_counter(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, x counter, y counter, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        cql.execute(f"UPDATE {table} SET y = y + 1 WHERE p = {p} AND c = 1")
        assert list(cql.execute(f"SELECT x FROM {table} WHERE p = {p}")) == [(None,)]


# The view row marker is derived from the expiring v cell. Once it expires, with no
# tombstone involved, the dead marker must shadow the still-live unselected w cell.
def test_view_row_with_expired_marker_and_live_unselected_cell(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int, w int") as table:
        with new_materialized_view(cql, table, "*", "v, p", "v IS NOT NULL AND p IS NOT NULL") as mv:
            p = unique_key_int()
            cql.execute(f"INSERT INTO {table} (p, w) VALUES ({p}, 1)")
            cql.execute(f"UPDATE {table} USING TTL 5 SET v = 1 WHERE p = {p}")
            deadline = time.time() + 20
            while not list(cql.execute(f"SELECT p FROM {mv} WHERE v = 1")):
                assert time.time() < deadline
                time.sleep(0.1)
            deadline = time.time() + 20
            while list(cql.execute(f"SELECT p FROM {mv} WHERE v = 1")):
                assert time.time() < deadline
                time.sleep(0.1)
            assert list(cql.execute(f"SELECT w FROM {table} WHERE p = {p}")) == [(1,)]


# Reversed reads apply the same rules, from memtable, sstables and both merged.
def test_reversed_row_liveness_through_unselected_cells(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, a int, b int, s set<text>, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = 1")
        cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = 2")
        cql.execute(f"DELETE b FROM {table} WHERE p = {p} AND c = 2")
        cql.execute(f"UPDATE {table} SET s = {{'x'}} WHERE p = {p} AND c = 3")
        cql.execute(f"INSERT INTO {table} (p, c, a) VALUES ({p}, 4, 4)")
        cql.execute(f"UPDATE {table} USING TIMESTAMP 1 SET b = 1 WHERE p = {p} AND c = 5")
        select = f"SELECT c, a FROM {table} WHERE p = {p} ORDER BY c DESC"
        assert list(cql.execute(select)) == [(5, None), (4, 4), (3, None), (1, None)]
        nodetool.flush(cql, table)
        assert list(cql.execute(select)) == [(5, None), (4, 4), (3, None), (1, None)]
        cql.execute(f"DELETE FROM {table} USING TIMESTAMP 2 WHERE p = {p} AND c = 5")
        cql.execute(f"UPDATE {table} SET s = s - {{'x'}} WHERE p = {p} AND c = 3")
        assert list(cql.execute(select)) == [(4, 4), (1, None)]


# Paging must neither skip nor repeat rows that live only through unselected cells.
@pytest.mark.parametrize('order', ['', ' ORDER BY c DESC'], ids=['forward', 'reversed'])
def test_paging_rows_live_only_through_unselected_cells(cql, test_keyspace, order):
    with new_test_table(cql, test_keyspace, "p int, c int, a int, b int, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        for c in range(20):
            cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = {c}")
        statement = SimpleStatement(f"SELECT c, a FROM {table} WHERE p = {p}{order}", fetch_size=7)
        expected = [(c, None) for c in (reversed(range(20)) if order else range(20))]
        assert list(cql.execute(statement)) == expected


def test_count_rows_live_only_through_unselected_cells(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, a int, b int, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        for c in range(3):
            cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = {c}")
        cql.execute(f"DELETE b FROM {table} WHERE p = {p} AND c = 2")
        assert list(cql.execute(f"SELECT count(*) FROM {table} WHERE p = {p}")) == [(2,)]
        assert list(cql.execute(f"SELECT count(a) FROM {table} WHERE p = {p}")) == [(0,)]
        assert list(cql.execute(f"SELECT count(*) FROM {table}")) == [(2,)]


# A scan over all shards, not a single-partition read.
def test_scan_rows_live_only_through_unselected_cells(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, a int, b int, PRIMARY KEY (p, c)") as table:
        for p in range(10):
            cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = 1")
        cql.execute(f"DELETE b FROM {table} WHERE p = 0 AND c = 1")
        assert sorted(cql.execute(f"SELECT p, a FROM {table}")) == [(p, None) for p in range(1, 10)]


# The filtered column is not in the result, but must still be read.
def test_filter_on_unselected_column(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, a int, b int, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = 1")
        cql.execute(f"INSERT INTO {table} (p, c, a, b) VALUES ({p}, 2, 2, 2)")
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p = {p} AND b = 1 ALLOW FILTERING")) == [(1,)]
        assert list(cql.execute(f"SELECT a FROM {table} WHERE p = {p} AND b = 2 ALLOW FILTERING")) == [(2,)]


# IF EXISTS reads no columns, yet a row alive only through a regular cell exists.
def test_lwt_if_exists_row_live_only_through_cell(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, a int, b int, PRIMARY KEY (p, c)") as table:
        p = unique_key_int()
        cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = 1")
        assert list(cql.execute(f"UPDATE {table} SET a = 1 WHERE p = {p} AND c = 1 IF EXISTS"))[0][0]
        cql.execute(f"DELETE b FROM {table} WHERE p = {p} AND c = 2")
        assert not list(cql.execute(f"UPDATE {table} SET a = 1 WHERE p = {p} AND c = 2 IF EXISTS"))[0][0]
        cql.execute(f"UPDATE {table} SET b = 1 WHERE p = {p} AND c = 3")
        cql.execute(f"DELETE b FROM {table} WHERE p = {p} AND c = 3")
        assert not list(cql.execute(f"UPDATE {table} SET a = 1 WHERE p = {p} AND c = 3 IF EXISTS"))[0][0]
