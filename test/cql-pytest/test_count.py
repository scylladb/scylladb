# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the COUNT() aggregation function
#############################################################################

import pytest
from util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        yield table

# When there is no row matching the selection, the count should be 0.
# First check a "=" expression matching no row:
def test_count_empty_eq(cql, table1):
    p = unique_key_int()
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))


# Now check an IN with an empty list. Reproduces #12475.
@pytest.mark.xfail(reason="issue #12475")
def test_count_empty_in(cql, table1):
    assert [(0,)] == list(cql.execute(f"select count(*) from {table1} where p in ()"))

# Simple test of counting the number of rows in a single partition
def test_count_in_partition(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    assert [(3,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))

# Using count(v) instead of count(*) allows counting only rows with a set
# value in v
def test_count_specific_column(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    cql.execute(stmt, [p, 4, None])
    assert [(4,)] == list(cql.execute(f"select count(*) from {table1} where p = {p}"))
    assert [(3,)] == list(cql.execute(f"select count(v) from {table1} where p = {p}"))

# COUNT can be combined with GROUP BY to count separately for each partition
# or row.
def test_count_and_group_by_row(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p, 1, 1])
    cql.execute(stmt, [p, 2, 2])
    cql.execute(stmt, [p, 3, 3])
    cql.execute(stmt, [p, 4, None])
    assert [(p, 1, 1), (p, 2, 1), (p, 3, 1), (p, 4, 0)] == list(cql.execute(f"select p, c, count(v) from {table1} where p = {p} group by p,c"))

def test_count_and_group_by_partition(cql, table1):
    p1 = unique_key_int()
    p2 = unique_key_int()
    stmt = cql.prepare(f"insert into {table1} (p, c, v) values (?, ?, ?)")
    cql.execute(stmt, [p1, 1, 1])
    cql.execute(stmt, [p1, 2, 2])
    cql.execute(stmt, [p2, 3, 3])
    cql.execute(stmt, [p2, 4, None])
    assert [(p1, 2), (p2, 1)] == list(cql.execute(f"select p, count(v) from {table1} where p in ({p1},{p2}) group by p"))

# In the above tests we looked for per-row or per-partition counts and got
# back more than one count. But if our query matches no row, we should get
# back no count.
@pytest.mark.xfail(reason="issue #12477")
def test_count_and_group_by_row_none(cql, table1):
    p = unique_key_int()
    assert [] == list(cql.execute(f"select p, c, count(v) from {table1} where p = {p} group by p,c"))

@pytest.mark.xfail(reason="issue #12477")
def test_count_and_group_by_partition_none(cql, table1):
    p = unique_key_int()
    assert [] == list(cql.execute(f"select p, count(v) from {table1} where p = {p} group by p"))
