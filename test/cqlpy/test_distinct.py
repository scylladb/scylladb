# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the SELECT DISTINCT feature
#############################################################################

from test import DEBUG_MODES
import pytest
from .util import config_value_context, new_named_table, new_test_table, unique_key_int
from .nodetool import del_snapshot, list_snapshots
from cassandra.protocol import InvalidRequest
from cassandra.query import FETCH_SIZE_UNSET, SimpleStatement

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        yield table

# Simple test for SELECT DISTINCT inside a single partition (returning just
# that single partition, or nothing)
def test_distinct_in_partition(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    # Without DISTINCT, three matches, with DISTINCT just one:
    assert [(p,),(p,),(p,)] == list(cql.execute(f"select p from {table1} where p = {p}"))
    assert [(p,)] == list(cql.execute(f"select distinct p from {table1} where p = {p}"))
    p2 = unique_key_int()
    assert [] == list(cql.execute(f"select distinct p from {table1} where p = {p2}"))

# When we have "select distinct p", adding a "group by p" without any
# aggregation function is allowed, but doesn't change anything:
def test_distinct_in_partition_group_by(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    assert [(p,)] == list(cql.execute(f"select distinct p from {table1} where p = {p} group by p"))
    p2 = unique_key_int()
    assert [] == list(cql.execute(f"select distinct p from {table1} where p = {p2} group by p"))

# "select distinct p" with "group by p,c" doesn't makes sense (we always
# have one value for p, we can't split it per c), and should not be allowed.
def test_distinct_in_partition_group_by_c(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    # Cassandra reports the error message: "Grouping on clustering columns
    # is not allowed for SELECT DISTINCT queries".
    with pytest.raises(InvalidRequest, match='SELECT DISTINCT'):
        cql.execute(f"select distinct p from {table1} where p = {p} group by p,c")

# Test combination of SELECT DISTINCT with LIMIT
# This test involves a whole-table scan, so we need a fresh table.
def test_distinct_limit(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"insert into {table} (p, c, v) values (?, ?, ?)")
        N = 10
        ps = [unique_key_int() for i in range(N)]
        for i in range(N):
            cql.execute(stmt, [ps[i], 1, 7])
            cql.execute(stmt, [ps[i], 2, 7])
        # SELECT DISTINCT should produce the N results all:
        all = [(p,) for p in ps]
        assert sorted(all) == sorted(list(cql.execute(f"select distinct p from {table}")))
        # WITH LIMIT 0<n<=N we should get only n results
        for i in range(N):
            n = i + 1
            results = list(cql.execute(f"select distinct p from {table} limit {n}"))
            assert n == len(results)
            assert set(results).issubset(set(all))

# Test that SELECT DISTINCT and PER PARTITION LIMIT is not allowed in the same query
# reproduces #15109
def test_distinct_and_per_partition_limit(cql, table1):
    with pytest.raises(InvalidRequest, match='PER PARTITION LIMIT is not allowed with SELECT DISTINCT queries'):
        cql.execute(f"select distinct p from {table1} per partition limit 1")

# Test combination of SELECT DISTINCT, COUNT, GROUP BY and LIMIT.
# COUNT + GROUP BY means generate one count per partition, and the
# SELECT DISTINCT simply hands the counter just one row per partition
# to count, so all the counts come up 1. Adding a LIMIT to all of
# this exposes the same bug of limiting COUNT + GROUP BY, without the
# SELECT DISTINCT, reported in issue #5361:
def test_distinct_count_group_by_limit(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"insert into {table} (p, c, v) values (?, ?, ?)")
        N = 10
        ps = [unique_key_int() for i in range(N)]
        for i in range(N):
            cql.execute(stmt, [ps[i], 1, 7])
            cql.execute(stmt, [ps[i], 2, 7])
        # SELECT DISTINCT should produce the N results ps:
        all = [(p,1) for p in ps]
        assert sorted(all) == sorted(list(cql.execute(f"select distinct p, count(p) from {table} group by p")))
        for i in range(N):
            n = i + 1
            results = list(cql.execute(f"select distinct p, count(p) from {table} group by p limit {n}"))
            assert n == len(results)
            assert set(results).issubset(set(all))

# A paged SELECT DISTINCT over the whole table, after a partition was
# deleted, returns the same partitions as the unpaged one.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_distinct_paged_with_deleted_partition(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "k int PRIMARY KEY, c int, v int") as table:
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (k, c, v) VALUES ({i}, {i}, {i})")
        cql.execute(f"DELETE FROM {table} WHERE k = 3")
        expected = list(cql.execute(f"SELECT DISTINCT k FROM {table}"))
        assert len(expected) == 9
        for page_size in [1, 4, 5, 9, 10]:
            assert expected == list(cql.execute(SimpleStatement(f"SELECT DISTINCT k FROM {table}", fetch_size=page_size)))

# Ported from scylla-dtest cql_additional_tests.py::TestCQL::test_static_columns_with_distinct.
# SELECT DISTINCT of the partition key and a static column, on partitions
# with and without a static value; SELECT DISTINCT of only a static column
# without restricting the partition key is rejected. Then, after a TRUNCATE
# (auto_snapshot is on for it, as on the dtest's node, and the test removes
# the snapshot), a paged SELECT DISTINCT of the partition key and static
# columns, over the whole table, with LIMIT and with IN, returns each
# partition once, with its static values, for any page size (None is
# unpaged). Reproduces CASSANDRA-8108 and CASSANDRA-8087.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("test_keyspace", ["vnodes"], indirect=True)
def test_distinct_static_columns_paged(cql, test_keyspace):
    def rows(stmt, fetch_size=FETCH_SIZE_UNSET):
        return [tuple(r) for r in cql.execute(SimpleStatement(stmt, fetch_size=fetch_size))]
    with new_named_table(cql, test_keyspace, "test", "k int, p int, s int static, PRIMARY KEY (k, p)") as table:
        cql.execute(f"INSERT INTO {table} (k, p) VALUES (1, 1)")
        cql.execute(f"INSERT INTO {table} (k, p) VALUES (1, 2)")
        assert rows(f"SELECT k, s FROM {table}") == [(1, None), (1, None)]
        assert rows(f"SELECT DISTINCT k, s FROM {table}") == [(1, None)]
        assert rows(f"SELECT DISTINCT s FROM {table} WHERE k=1") == [(None,)]
        assert rows(f"SELECT DISTINCT s FROM {table} WHERE k=2") == []
        cql.execute(f"INSERT INTO {table} (k, p, s) VALUES (2, 1, 3)")
        cql.execute(f"INSERT INTO {table} (k, p) VALUES (2, 2)")
        assert rows(f"SELECT k, s FROM {table}") == [(1, None), (1, None), (2, 3), (2, 3)]
        assert rows(f"SELECT DISTINCT k, s FROM {table}") == [(1, None), (2, 3)]
        assert rows(f"SELECT DISTINCT s FROM {table} WHERE k=1") == [(None,)]
        assert rows(f"SELECT DISTINCT s FROM {table} WHERE k=2") == [(3,)]
        with pytest.raises(InvalidRequest):
            cql.execute(f"SELECT DISTINCT s FROM {table}")
        try:
            with config_value_context(cql, 'auto_snapshot', 'true'):
                cql.execute(f"TRUNCATE {table}")
        finally:
            for snapshot in list_snapshots(cql):
                if any(s['ks'] == test_keyspace and s['cf'] == 'test' for s in snapshot['value']):
                    del_snapshot(cql, snapshot['key'], [test_keyspace])
        for i in range(10):
            for j in range(10):
                cql.execute(f"INSERT INTO {table} (k, p, s) VALUES (%s, %s, %s)", (i, j, i))
        result = rows(f"SELECT DISTINCT k, s FROM {table}", 7)
        assert list(range(10)) == sorted([r[0] for r in result])
        assert list(range(10)) == sorted([r[1] for r in result])
        keys = ",".join(map(str, range(10)))
        result = rows(f"SELECT DISTINCT k, s FROM {table} WHERE k IN ({keys})", 7)
        assert list(range(10)) == [r[0] for r in result]
        assert list(range(10)) == [r[1] for r in result]
        with new_named_table(cql, test_keyspace, "test2", "k int, c1 int, c2 int, s1 int static, s2 int static, PRIMARY KEY (k, c1, c2)") as table2:
            for i in range(10):
                for j in range(5):
                    for k in range(5):
                        cql.execute(f"INSERT INTO {table2} (k, c1, c2, s1, s2) VALUES (%s, %s, %s, %s, %s)", (i, j, k, i, i + 1))
            for fetch_size in [None, 2, 5, 7, 10, 24, 25, 26, 1000]:
                def select(query):
                    return rows(query, fetch_size)
                result = select(f"SELECT DISTINCT k, s1 FROM {table2}")
                assert list(range(10)) == sorted([r[0] for r in result])
                assert list(range(10)) == sorted([r[1] for r in result])
                result = select(f"SELECT DISTINCT k, s2 FROM {table2}")
                assert list(range(10)) == sorted([r[0] for r in result])
                assert list(range(1, 11)) == sorted([r[1] for r in result])
                result = select(f"SELECT DISTINCT k, s1 FROM {table2} LIMIT 10")
                assert list(range(10)) == sorted([r[0] for r in result])
                assert list(range(10)) == sorted([r[1] for r in result])
                result = select(f"SELECT DISTINCT k, s1 FROM {table2} WHERE k IN ({keys})")
                assert list(range(10)) == [r[0] for r in result]
                assert list(range(10)) == [r[1] for r in result]
                result = select(f"SELECT DISTINCT k, s2 FROM {table2} WHERE k IN ({keys})")
                assert list(range(10)) == [r[0] for r in result]
                assert list(range(1, 11)) == [r[1] for r in result]
                result = select(f"SELECT DISTINCT k, s1 FROM {table2} WHERE k IN ({keys}) LIMIT 10")
                assert list(range(10)) == sorted([r[0] for r in result])
                assert list(range(10)) == sorted([r[1] for r in result])


# Tests ported from scylla-dtest's cql_additional_tests.py. Like the dtests,
# they run in a keyspace with vnodes, and keep the dtests' table names.

# SELECT DISTINCT of the partition key with LIMIT, on tables with a
# composite partition key with and without a clustering key, and with a
# text clustering key; selecting only part of the partition key, or a
# clustering column, is rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_select_distinct_with_limit(cql, test_keyspace_vnodes):
    with new_named_table(cql, test_keyspace_vnodes, "regular", "pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0)") as regular:
        for i in range(3):
            cql.execute(f"INSERT INTO {regular} (pk0, pk1, ck0, val) VALUES ({i}, {i}, 0, 0)")
            cql.execute(f"INSERT INTO {regular} (pk0, pk1, ck0, val) VALUES ({i}, {i}, 1, 1)")
        assert list(cql.execute(f"SELECT DISTINCT pk0, pk1 FROM {regular} LIMIT 1")) == [(0, 0)]
        assert sorted(cql.execute(f"SELECT DISTINCT pk0, pk1 FROM {regular} LIMIT 3")) == [(0, 0), (1, 1), (2, 2)]
        with new_named_table(cql, test_keyspace_vnodes, "compact", "pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))") as compact:
            for i in range(3):
                cql.execute(f"INSERT INTO {compact} (pk0, pk1, val) VALUES ({i}, {i}, {i})")
            assert list(cql.execute(f"SELECT DISTINCT pk0, pk1 FROM {compact} LIMIT 1")) == [(0, 0)]
            assert sorted(cql.execute(f"SELECT DISTINCT pk0, pk1 FROM {compact} LIMIT 3")) == [(0, 0), (1, 1), (2, 2)]
        with new_named_table(cql, test_keyspace_vnodes, "wide", "pk int, name text, val int, PRIMARY KEY(pk, name)") as wide:
            for i in range(3):
                cql.execute(f"INSERT INTO {wide} (pk, name, val) VALUES ({i}, 'name0', 0)")
                cql.execute(f"INSERT INTO {wide} (pk, name, val) VALUES ({i}, 'name1', 1)")
            assert list(cql.execute(f"SELECT DISTINCT pk FROM {wide} LIMIT 1")) == [(1,)]
            assert sorted(cql.execute(f"SELECT DISTINCT pk FROM {wide} LIMIT 3")) == [(0,), (1,), (2,)]
        with pytest.raises(InvalidRequest, match="queries must request all the partition key columns"):
            cql.execute(f"SELECT DISTINCT pk0 FROM {regular}")
        with pytest.raises(InvalidRequest, match="queries must only request partition key columns"):
            cql.execute(f"SELECT DISTINCT pk0, pk1, ck0 FROM {regular}")
