# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the SELECT DISTINCT feature
#############################################################################

import pytest
from util import new_test_table, unique_key_int, random_string
from cassandra.protocol import InvalidRequest

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
