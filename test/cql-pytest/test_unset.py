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

# Try doing UPDATE table4 SET c=UNSET_VALUE
# Should fail, clustering columns can't be updated
def test_update_unset_clustering_column(cql, table4):
    with pytest.raises(InvalidRequest):
        cql.prepare(f"UPDATE {table4} SET r=123, c = ? WHERE p = 0 AND c = ?")
        # Prepare fails, no point in executing it.

# Test doing UPDATE table4 SET s=UNSET_VALUE
def test_update_unset_static_column(cql, table4, scylla_only):
    p = unique_key_int()
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    # UPDATE SET s=UNSET_VALUE
    update1 = cql.prepare(f"UPDATE {table4} SET s=? WHERE p = {p} AND c = ?")
    cql.execute(update1, [UNSET_VALUE, 1])
    assert select_rows() == []

    # Try the same with c = UNSET_VALUE
    cql.execute(update1, [UNSET_VALUE, UNSET_VALUE])
    assert select_rows() == []

    # UPDATE SET s=UNSET_VALUE, r=123
    update2 = cql.prepare(f"UPDATE {table4} SET r=123, s=? WHERE p = {p} AND c = ?")
    cql.execute(update2, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, None, 123)]

    # Try the same with c = UNSET_VALUE
    cql.execute(update2, [UNSET_VALUE, UNSET_VALUE])
    assert select_rows() == [(p, 1, None, 123)]

    # UPDATE SET s=4321
    update3 = cql.prepare(f"UPDATE {table4} SET s=4321 WHERE p = {p} AND c = ?")
    cql.execute(update3, [1])
    assert select_rows() == [(p, 1, 4321, 123)]

    # Try the same with c = UNSET_VALUE
    cql.execute(update3, [UNSET_VALUE])
    assert select_rows() == [(p, 1, 4321, 123)]

    # UPDATE SET r=567, s=UNSET_VALUE
    update4 = cql.prepare(f"UPDATE {table4} SET r=567, s=? WHERE p = {p} AND c = ?")
    cql.execute(update4, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 4321, 567)]

    # Try the same with c = UNSET_VALUE
    cql.execute(update4, [UNSET_VALUE, UNSET_VALUE])
    assert select_rows() == [(p, 1, 4321, 567)]

# Test doing UPDATE table4 SET r=UNSET_VALUE
def test_update_unset_regular_column(cql, table4, scylla_only):
    p = unique_key_int()
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    # UPDATE SET r = UNSET_VALUE
    update1 = cql.prepare(f"UPDATE {table4} SET r = ? WHERE p = {p} AND c = ?")
    cql.execute(update1, [UNSET_VALUE, 1])
    assert select_rows() == []

    # Try the same with c = UNSET_VALUE
    cql.execute(update1, [UNSET_VALUE, UNSET_VALUE])
    assert select_rows() == []

    # UPDATE SET r = UNSET_VALUE, s = 100
    update2 = cql.prepare(f"UPDATE {table4} SET r = ?, s = 100 WHERE p = {p} AND c = ?")
    cql.execute(update2, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 100, None)]

    # Try the same with c = UNSET_VALUE
    cql.execute(update2, [UNSET_VALUE, UNSET_VALUE])
    assert select_rows() == [(p, 1, 100, None)]

    # UPDATE SET r = 200
    update3 = cql.prepare(f"UPDATE {table4} SET r = 200 WHERE p = {p} AND c = ?")
    cql.execute(update3, [1])
    assert select_rows() == [(p, 1, 100, 200)]

    # Try the same with c = UNSET_VALUE
    cql.execute(update3, [UNSET_VALUE])
    assert select_rows() == [(p, 1, 100, 200)]

    # UPDATE SET r = UNSET_VALUE, s = 300
    update4 = cql.prepare(f"UPDATE {table4} SET r = ?, s = 300 WHERE p = {p} AND c = ?")
    cql.execute(update4, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 300, 200)]

    # Try the same with c = UNSET_VALUE
    cql.execute(update4, [UNSET_VALUE, UNSET_VALUE])
    assert select_rows() == [(p, 1, 300, 200)]

# Test doing UPDATE table4 SET s=UNSET_VALUE IF EXISTS
def test_update_unset_static_column_if_exists(cql, table4):
    p = unique_key_int()
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    # UPDATE SET s=UNSET_VALUE
    update1 = cql.prepare(f"UPDATE {table4} SET s=? WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update1, [UNSET_VALUE, 1])
    assert select_rows() == []

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [UNSET_VALUE, UNSET_VALUE])

    # Insert something into the table so that the updates actually change something
    cql.execute(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, 1, 2, 3)")

    # UPDATE SET s=UNSET_VALUE, r=123
    update2 = cql.prepare(f"UPDATE {table4} SET r=123, s=? WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update2, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 2, 123)]

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update2, [UNSET_VALUE, UNSET_VALUE])

    # UPDATE SET s=4321
    update3 = cql.prepare(f"UPDATE {table4} SET s=4321 WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update3, [1])
    assert select_rows() == [(p, 1, 4321, 123)]

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update3, [UNSET_VALUE])

    # UPDATE SET r=567, s=UNSET_VALUE
    update4 = cql.prepare(f"UPDATE {table4} SET r=567, s=? WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update4, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 4321, 567)]

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update4, [UNSET_VALUE, UNSET_VALUE])

# Test doing UPDATE table4 SET r=UNSET_VALUE
def test_update_unset_regular_column_if_exists(cql, table4):
    p = unique_key_int()
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    # UPDATE SET r = UNSET_VALUE
    update1 = cql.prepare(f"UPDATE {table4} SET r = ? WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update1, [UNSET_VALUE, 1])
    assert select_rows() == []

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [UNSET_VALUE, UNSET_VALUE])

    # Insert something into the table so that the updates actually change something
    cql.execute(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, 1, 2, 3)")

    # UPDATE SET r = UNSET_VALUE, s = 100
    update2 = cql.prepare(f"UPDATE {table4} SET r = ?, s = 100 WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update2, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 100, 3)]

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update2, [UNSET_VALUE, UNSET_VALUE])

    # UPDATE SET r = 200
    update3 = cql.prepare(f"UPDATE {table4} SET r = 200 WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update3, [1])
    assert select_rows() == [(p, 1, 100, 200)]

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update3, [UNSET_VALUE])

    # UPDATE SET r = UNSET_VALUE, s = 300
    update4 = cql.prepare(f"UPDATE {table4} SET r = ?, s = 300 WHERE p = {p} AND c = ? IF EXISTS")
    cql.execute(update4, [UNSET_VALUE, 1])
    assert select_rows() == [(p, 1, 300, 200)]

    # Try the same with c = UNSET_VALUE
    with pytest.raises(InvalidRequest):
        cql.execute(update4, [UNSET_VALUE, UNSET_VALUE])

# Test doing UPDATE table4 SET s=UNSET_VALUE IF <lwt condition>, unset values in lwt condition are also tested
def test_update_unset_static_column_with_lwt(cql, table4):
    p = unique_key_int()
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    update1 = cql.prepare(f"UPDATE {table4} SET s = ? WHERE p = {p} AND c = ? IF s = ? AND r = ?")

    # UPDATE SET s = 123 WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [123, 123, 123, 123])
    assert select_rows() == []

    # UPDATE SET s = UNSET_VALUE WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [UNSET_VALUE, 123, 123, 123])
    assert select_rows() == []

    # Insert something into the table so that the updates actually change something
    cql.execute(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, 123, 123, 123)")

    # UPDATE SET s = UNSET_VALUE WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [UNSET_VALUE, 123, 123, 123])
    assert select_rows() == [(p, 123, 123, 123)]

    # UPDATE table4 SET s = 321 WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [321, 123, 123, 123])
    assert select_rows() == [(p, 123, 321, 123)]

    # Setting c (clustering column) to UNSET_VALUE should generate an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [123, UNSET_VALUE, 123, 123])

    # Doing IF s = UNSET generates an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [9000000, 123, UNSET_VALUE, 123])

    # Doing IF r = UNSET silently skips the update
    cql.execute(update1, [9000000, 123, 123, UNSET_VALUE])
    assert select_rows() == [(p, 123, 321, 123)]

    # Doing IF s = UNSET AND r = UNSET generates an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [9000000, 123, UNSET_VALUE, UNSET_VALUE])

    # Setting everything to UNSET generates an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE])

# Test doing UPDATE table4 SET r=UNSET_VALUE IF <lwt condition>, unset values in lwt condition are also tested
def test_update_unset_regular_column_with_lwt(cql, table4):
    p = unique_key_int()
    def select_rows():
        return list(cql.execute(f"SELECT p, c, s, r FROM {table4} WHERE p = {p}"))

    update1 = cql.prepare(f"UPDATE {table4} SET r = ? WHERE p = {p} AND c = ? IF s = ? AND r = ?")

    # UPDATE SET r = 123 WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [123, 123, 123, 123])
    assert select_rows() == []

    # UPDATE SET r = UNSET_VALUE WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [UNSET_VALUE, 123, 123, 123])
    assert select_rows() == []

    # Insert something into the table so that the updates actually change something
    cql.execute(f"INSERT INTO {table4} (p, c, s, r) VALUES ({p}, 123, 123, 123)")

    # UPDATE SET r = UNSET_VALUE WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [UNSET_VALUE, 123, 123, 123])
    assert select_rows() == [(p, 123, 123, 123)]

    # UPDATE table4 SET r = 321 WHERE c = 123 AND s = 123 AND r = 123
    cql.execute(update1, [321, 123, 123, 123])
    assert select_rows() == [(p, 123, 123, 321)]

    # Setting c (clustering column) to UNSET_VALUE should generate an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [123, UNSET_VALUE, 123, 123])

    # Doing IF s = UNSET generates an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [9000000, 123, UNSET_VALUE, 123])

    # Doing IF r = UNSET generates an error
    # This didn't cause an error when updating s instead of r
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [9000000, 123, 123, UNSET_VALUE])

    # Doing IF s = UNSET AND r = UNSET generates an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [9000000, 123, UNSET_VALUE, UNSET_VALUE])

    # Setting everything to UNSET generates an error
    with pytest.raises(InvalidRequest):
        cql.execute(update1, [UNSET_VALUE, UNSET_VALUE, UNSET_VALUE, UNSET_VALUE])
