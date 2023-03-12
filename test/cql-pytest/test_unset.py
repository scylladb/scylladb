# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from util import new_test_table, unique_key_int
from cassandra.query import UNSET_VALUE
from cassandra.protocol import InvalidRequest

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, a int, b int, c int, li list<int>") as table:
        yield table

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        yield table

@pytest.fixture(scope="module")
def table3(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, r int, PRIMARY KEY (p, c)") as table:
        yield table

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


# A basic test that in a prepared statement with three assignments, one
# bound by an UNSET_VALUE is simply not done, but the other ones are.
# Try all 2^3 combinations of a 3 column updates with each one set to either
# a real value or an UNSET_VALUE.
def test_update_unset_value_basic(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} SET a=?, b=?, c=? WHERE p={p}')
    a = 1
    b = 2
    c = 3
    cql.execute(stmt, [a, b, c])
    assert [(a, b, c)] == list(cql.execute(f'SELECT a,b,c FROM {table1} WHERE p = {p}'))
    i = 4
    for unset_a in [False, True]:
        for unset_b in [False, True]:
            for unset_c in [False, True]:
                if unset_a:
                    newa = UNSET_VALUE
                else:
                    newa = i
                    a = i
                    i += 1
                if unset_b:
                    newb = UNSET_VALUE
                else:
                    newb = i
                    b = i
                    i += 1
                if unset_c:
                    newc = UNSET_VALUE
                else:
                    newc = i
                    c = i
                    i += 1
                cql.execute(stmt, [newa, newb, newc])
                assert [(a, b, c)] == list(cql.execute(f'SELECT a,b,c FROM {table1} WHERE p = {p}'))

# The expression "SET a=?" is skipped if the bound value is UNSET_VALUE.
# But what if it is part of a more complex expression like "SET a=(int)?+1"
# (arithmetic expression on the bind variable)? Does the SET also get
# skipped? Cassandra, and Scylla, decided that the answer will be no:
# We refuse to evaluate expressions involving an UNSET_VALUE, and in
# such case the whole write request will fail instead of parts of it being
# skipped. See discussion in pull request #12517.

@pytest.mark.xfail(reason="issue #2693 - Scylla doesn't yet support arithmetic expressions")
def test_update_unset_value_expr_arithmetic(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} SET a=(int)?+1 WHERE p={p}')
    cql.execute(stmt, [7])
    assert [(8,)] == list(cql.execute(f'SELECT a FROM {table1} WHERE p = {p}'))
    with pytest.raises(InvalidRequest):
        cql.execute(stmt, [UNSET_VALUE])

# Despite the decision that expressions will not allow UNSET_VALUE, Cassandra
# decided that (quoting its NEWS.txt) "an unset bind counter operation does
# not change the counter value.".  So "c = c + ?" for a counter, when given
# an UNSET_VALUE, will causes the write to be skipped, without error.
# The rationale is that "c = c + ?" is not an expression - it doesn't actually
# calculate c + ?, but rather it is a primitive increment operation, and
# passing ?=UNSET_VALUE should be able to skip this primitive operation.
def test_unset_counter_increment(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, c counter") as table:
        p = unique_key_int()
        stmt = cql.prepare(f'UPDATE {table} SET c=c+? WHERE p={p}')
        cql.execute(stmt, [3])
        assert [(3,)] == list(cql.execute(f'SELECT c FROM {table} WHERE p = {p}'))
        cql.execute(stmt, [UNSET_VALUE])
        assert [(3,)] == list(cql.execute(f'SELECT c FROM {table} WHERE p = {p}'))

# Like the counter increment, a list append operation (li=li+?) is a primitive
# operation and not expression, so we believe UNSET_VALUE should be able
# to skip it, and Scylla indeed does as this test shows. Cassandra fails
# this test - it produces an internal error on a bad cast, and we consider
# this a Cassandra bug and hence the cassandra_bug tag.
def test_unset_list_append(cql, table1, cassandra_bug):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} SET li=li+? WHERE p={p}')
    cql.execute(stmt, [[7]])
    assert [([7],)] == list(cql.execute(f'SELECT li FROM {table1} WHERE p = {p}'))
    cql.execute(stmt, [UNSET_VALUE])
    assert [([7],)] == list(cql.execute(f'SELECT li FROM {table1} WHERE p = {p}'))

# According to Cassandra's NEWS.txt, "an unset bind ttl is treated as
# 'unlimited'". It shouldn't skip the write.
# Note that the NEWS.txt is not accurate: An unset ttl isn't really treated
# as unlimited, but rather as the default ttl set on the table. The default
# ttl is usually unlimited, but not always. We test that case in
# test_ttl.py::test_default_ttl_unset()
def test_unset_ttl(cql, table1):
    p = unique_key_int()
    # First write using a normal TTL:
    stmt = cql.prepare(f'UPDATE {table1} USING TTL ? SET a=? WHERE p={p}')
    cql.execute(stmt, [20000, 3])
    res = list(cql.execute(f'SELECT a, ttl(a) FROM {table1} WHERE p = {p}'))
    assert res[0].a == 3
    assert res[0].ttl_a > 10000
    # Check that an UNSET_VALUE ttl didn't skip the write but reset the TTL
    # to unlimited (None)
    cql.execute(stmt, [UNSET_VALUE, 4])
    assert [(4, None)] == list(cql.execute(f'SELECT a, ttl(a) FROM {table1} WHERE p = {p}'))

# According to Cassadra's NEWS.txt, "an unset bind timestamp is treated
# as 'now'". It shouldn't skip the write.
def test_unset_timestamp(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'UPDATE {table1} USING TIMESTAMP ? SET a=? WHERE p={p}')
    cql.execute(stmt, [UNSET_VALUE, 3])
    assert [(3,)] == list(cql.execute(f'SELECT a FROM {table1} WHERE p = {p}'))

# According to Cassandra's NEWS.txt, "In a QUERY request an unset limit
# is treated as 'unlimited'.". It mustn't cause the query to fail (let alone
# be skipped somehow).
def test_unset_limit(cql, table2):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 1)')
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 2)')
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 3)')
    cql.execute(f'INSERT INTO {table2} (p, c) VALUES ({p}, 4)')
    stmt = cql.prepare(f'SELECT c FROM {table2} WHERE p={p} limit ?')
    assert [(1,),(2,)] == list(cql.execute(stmt, [2]))
    assert [(1,),(2,),(3,),(4,)] == list(cql.execute(stmt, [UNSET_VALUE]))

# TODO: check that (according to NEWS.txt documentation): "Unset tuple field,
# UDT field and map key are not allowed.".

# Similar to test_unset_insert_where() above, just use an LWT write ("IF
# NOT EXISTS"). Test that using an UNSET_VALUE in an LWT condtion causes
# a clear error, not silent skip and not a crash as in issue #13001.
def test_unset_insert_where_lwt(cql, table2):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table2} (p, c) VALUES ({p}, ?) IF NOT EXISTS')
    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE])

# Like test_unset_insert_where_lwt, but using UPDATE
# Python driver doesn't allow sending an UNSET_VALUE for the partition key,
# so only the clustering key is tested.
def test_unset_update_where_lwt(cql, table3):
    stmt = cql.prepare(f"UPDATE {table3} SET r = 42 WHERE p = 0 AND c = ? IF r = ?")

    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [UNSET_VALUE, 2])

    with pytest.raises(InvalidRequest, match="unset"):
        cql.execute(stmt, [1, UNSET_VALUE])
