# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from util import new_test_table, unique_key_int
from cassandra.query import UNSET_VALUE
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table4(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, s int, r int, PRIMARY KEY (p, c)") as table:
        yield table

# Test INSERT with UNSET_VALUE for the clustering column value
def test_insert_unset_clustering_col(cql, table4, scylla_only):
    p = unique_key_int()
    def insert(c, s, r):
        cql.execute(cql.prepare(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, ?, ?, ?)"), [c, s, r])
    def select_rows():
        return sorted(list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}")))

    # INSERT (p, UNSET, 2, 3)
    insert(c=UNSET_VALUE, s=2, r=3)
    assert select_rows() == []

    # INSERT (p, 1, 2, 3)
    insert(c=1, s=2, r=3)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, UNSET, 2, 3)
    insert(c=UNSET_VALUE, s=2, r=3)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, UNSET, 5, 6)
    insert(c=UNSET_VALUE, s=5, r=6)
    assert select_rows() == [(p, 1, 2, 3)]

# Test INSERT with UNSET_VALUE for the static column value
def test_insert_unset_static_col(cql, table4):
    p = unique_key_int()
    def insert(c, s, r):
        cql.execute(cql.prepare(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, ?, ?, ?)"), [c, s, r])
    def select_rows():
        return sorted(list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}")))

    # INSERT (p, 1, UNSET, 3)
    insert(c=1, s=UNSET_VALUE, r=3)
    assert select_rows() == [(p, 1, None, 3)]

    # INSERT (p, 1, 2, 3)
    insert(c=1, s=2, r=3)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, 1, UNSET, 3)
    insert(c=1, s=UNSET_VALUE, r=3)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, 1, UNSET, 4)
    insert(c=1, s=UNSET_VALUE, r=4)
    assert select_rows() == [(p, 1, 2, 4)]

# Test INSERT with UNSET_VALUE for the regular column value
def test_insert_unset_regular_col(cql, table4):
    p = unique_key_int()
    def insert(c, s, r):
        cql.execute(cql.prepare(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, ?, ?, ?)"), [c, s, r])
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    # INSERT (p, 1, 2, UNSET)
    insert(c=1, s=2, r=UNSET_VALUE)
    assert select_rows() == [(p, 1, 2, None)]

    # INSERT (p, 1, 2, 3)
    insert(c=1, s=2, r=3)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, 1, 2, UNSET)
    insert(c=1, s=2, r=UNSET_VALUE)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, 1, 5, UNSET)
    insert(c=1, s=5, r=UNSET_VALUE)
    assert select_rows() == [(p, 1, 5, 3)]

# Test INSERT with UNSET_VALUE for the clustering column value, using IF NOT EXISTS
def test_insert_unset_clustering_col_if_not_exists(cql, table4):
    p = unique_key_int()
    def insert(c, s, r):
        cql.execute(cql.prepare(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, ?, ?, ?) IF NOT EXISTS"), [c, s, r])
    def select_rows():
        return sorted(list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}")))

    # INSERT (p, UNSET, 2, 3)
    with pytest.raises(InvalidRequest, match='unset'):
        insert(c=UNSET_VALUE, s=2, r=3)

    # INSERT (p, 1, 2, 3)
    insert(c=1, s=2, r=3)
    assert select_rows() == [(p, 1, 2, 3)]

    # INSERT (p, UNSET, 2, 3)
    with pytest.raises(InvalidRequest, match='unset'):
        insert(c=UNSET_VALUE, s=2, r=3)

    # INSERT (p, UNSET, 5, 6)
    with pytest.raises(InvalidRequest, match='unset'):
        insert(c=UNSET_VALUE, s=5, r=6)

    assert select_rows() == [(p, 1, 2, 3)]

# Test INSERT with UNSET_VALUE for the static column value, using IF NOT EXISTS
def test_insert_unset_static_col_if_not_exists(cql, table4):
    p = unique_key_int()
    def insert(c, s, r):
        cql.execute(cql.prepare(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, ?, ?, ?) IF NOT EXISTS"), [c, s, r])
    def select_rows():
        return sorted(list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}")))

    # INSERT (p, 1, UNSET, 3)
    insert(c=1, s=UNSET_VALUE, r=3)
    assert select_rows() == [(p, 1, None, 3)]

    # INSERT (p, 1, 2, 3)
    insert(c=1, s=2, r=3)
    assert select_rows() == [(p, 1, None, 3)]

    # INSERT (p, 1, UNSET, 3)
    insert(c=1, s=UNSET_VALUE, r=3)
    assert select_rows() == [(p, 1, None, 3)]

    # INSERT (p, 1, UNSET, 4)
    insert(c=1, s=UNSET_VALUE, r=4)
    assert select_rows() == [(p, 1, None, 3)]

# Test INSERT with UNSET_VALUE for the regular column value, using IF NOT EXISTS
def test_insert_unset_regular_col_if_not_exists(cql, table4):
    p = unique_key_int()
    def insert(c, s, r):
        cql.execute(cql.prepare(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, ?, ?, ?) IF NOT EXISTS"), [c, s, r])
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    # INSERT (p, 1, 2, UNSET)
    insert(c=1, s=2, r=UNSET_VALUE)
    assert select_rows() == [(p, 1, 2, None)]

    # INSERT (p, 1, 2, 3)
    insert(c=1, s=2, r=3)
    assert select_rows() == [(p, 1, 2, None)]

    # INSERT (p, 1, 2, UNSET)
    insert(c=1, s=2, r=UNSET_VALUE)
    assert select_rows() == [(p, 1, 2, None)]

    # INSERT (p, 1, 5, UNSET)
    insert(c=1, s=5, r=UNSET_VALUE)
    assert select_rows() == [(p, 1, 2, None)]
