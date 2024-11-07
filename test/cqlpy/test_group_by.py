# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for SELECT's GROUP BY feature
#############################################################################

import pytest
from util import new_test_table
from cassandra.protocol import InvalidRequest

# table1 has some pre-set data which the tests below SELECT on (the tests
# shouldn't write to it).
@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)",
                        extra="WITH default_time_to_live = 1000000") as table:
        stmt = cql.prepare(f'INSERT INTO {table} (p, c1, c2, v) VALUES (?, ?, ?, ?) USING TIMESTAMP ?')
        cql.execute(stmt, [0, 0, 0, 1, 100000001])
        cql.execute(stmt, [0, 0, 1, 2, 100000002])
        cql.execute(stmt, [0, 1, 0, 3, 100000003])
        cql.execute(stmt, [0, 1, 1, 4, 100000004])
        cql.execute(stmt, [1, 0, 0, 5, 100000005])
        cql.execute(stmt, [1, 0, 1, 6, 100000006])
        cql.execute(stmt, [1, 1, 0, 7, 100000007])
        cql.execute(stmt, [1, 1, 1, 8, 100000008])
        # FIXME: I would like to make the table read-only to prevent tests
        # from messing with it accidentally.
        yield table

### GROUP BY without aggregation:

# GROUP BY partition key picks a single row from each partition, the
# first row (in the clustering order).
def test_group_by_partition(cql, table1):
    assert {(0,0,0,1), (1,0,0,5)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p'))

# Adding a LIMIT should be honored.
# Reproduces #17237 - more results than the limit were generated.
def test_group_by_partition_with_limit(cql, table1):
    # "LIMIT 1" should return only one of the two matches (0,0,0,1) and
    # (1,0,0,5). The partition key 1 happens to be the first one in
    # murmur3 order, so this is what should be returned.
    assert {(1,0,0,5)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p LIMIT 1'))

def test_group_by_partition_count_with_limit(cql, test_keyspace):
    def execute_query(qry):
        # Test with a fetch size smaller than the number of rows in the result set
        stmt = cql.prepare(qry)
        stmt.fetch_size = 3
        return list(cql.execute(stmt))
    with new_test_table(cql, test_keyspace, "a int, b int, c int, PRIMARY KEY (a, b)") as table:
        # Insert a number rows into the table. 29, 31, 37, 41 as well as
        # 293, 317, 379, 419 are prime numbers and essentially magic numbers,
        # but they guaranteed to have no common factors with each other and
        # with LIMIT values we are going to use. They are also high enough to
        # avoid accidental collisions with the LIMIT values.
        for i in range(29):
            cql.execute(f"INSERT INTO {table} (a,b,c) VALUES (293,{i+1},293);");
        for i in range(31):
            cql.execute(f"INSERT INTO {table} (a,b,c) VALUES (317,{i+1},317);");
        for i in range(37):
            cql.execute(f"INSERT INTO {table} (a,b,c) VALUES (379,{i+1},379);");
        for i in range(41):
            cql.execute(f"INSERT INTO {table} (a,b,c) VALUES (419,{i+1},419);");

        assert [379, 293, 419, 317] == list(map(lambda x: x[0], execute_query(f'SELECT a FROM {table} GROUP BY a')))

        # Testing up to 5 to ensure that the correct results are returned if fewer
        # results are generated than the LIMIT
        for i in range(1,6):
            query_result = [x[0] for x in execute_query(f'SELECT a FROM {table} GROUP BY a LIMIT {i}')]
            assert query_result == [379, 293, 419, 317][:i]

        assert [37, 29, 41, 31] == [x[0] for x in execute_query(f'SELECT COUNT(a) FROM {table} GROUP BY a')]

        for i in range(1,5):
            assert [37, 29, 41, 31][:i] == [x[0] for x in execute_query(f'SELECT COUNT(a) FROM {table} GROUP BY a LIMIT {i}')]

# Try the same restricting the scan to a single partition instead of a
# whole-table scan. We should get just one row (the first row in the
# clustering order).
def test_group_by_partition_one_partition(cql, table1):
    assert {(0,0,0,1)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY p'))

# Same as previous test, but use ""SELECT *" instead of "SELECT p,c1,c2,v".
# This shouldn't make any difference, but reproduces bug #16531 where it did.
def test_group_by_partition_one_partition_star(cql, table1):
    assert {(0,0,0,1)} == set(cql.execute(f'SELECT * FROM {table1} WHERE p=0 GROUP BY p'))
    # Try the same with filtering, the GROUP BY should return the first row that
    # matches that filter.
    assert {(0,0,1,2)} == set(cql.execute(f'SELECT * FROM {table1} WHERE p=0 AND c2>0 GROUP BY p ALLOW FILTERING'))

# And if filtering excludes some rows, the GROUP BY should return the first
# row that matches the filter.
def test_group_by_partition_with_filtering(cql, table1):
    assert {(0,0,1,2), (1,0,1,6)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE c2>0 GROUP BY p ALLOW FILTERING'))

# Similarly, GROUP BY a partition key plus just a prefix of the clustering
# key retrieves the first row in each of the clustering ranges
def test_group_by_clustering_prefix(cql, table1):
    assert {(0,0,0,1), (0,1,0,3), (1,0,0,5), (1,1,0,7)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    # Try the same restricting the scan to a single partition instead of
    # a whole-table scan
    assert [(0,0,0,1), (0,1,0,3)] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY p,c1'))

# Adding a LIMIT should be honored.
# Reproduces #5362 - fewer results than the limit were generated.
def test_group_by_clustering_prefix_with_limit(cql, table1):
    results = list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    for i in range(1,4):
        assert results[:i] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 LIMIT {i}'))

# Same as the above test, but also add paging, and check that the LIMIT
# is handled correctly (limits the total number of returned results)
@pytest.mark.xfail(reason="issue #5362")
def test_group_by_clustering_prefix_with_limit_and_paging(cql, table1):
    results = list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    stmt = cql.prepare(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 LIMIT ?')
    page_size = 1
    stmt.fetch_size = page_size
    for i in range(1,4):
        r = cql.execute(stmt, [i])
        # First page should only have page_size rows. Then finish reading all
        # the pages, and expect the LIMIT to have worked.
        assert len(r.current_rows) == page_size
        assert results[:i] == list(r)

# Adding a PER PARTITION LIMIT should be honored
# Reproduces #5363 - fewer results than the limit were generated
@pytest.mark.xfail(reason="issue #5363")
def test_group_by_clustering_prefix_with_pplimit(cql, table1):
    # Without per-partition limit we get 4 results, 2 from each partition:
    assert {(0,0,0,1), (0,1,0,3), (1,0,0,5), (1,1,0,7)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1'))
    # With per-partition limit of 2, no change:
    # But in issue #5363, we got here fewer results.
    assert {(0,0,0,1), (0,1,0,3), (1,0,0,5), (1,1,0,7)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 2'))
    # With per-partition limit of 1, we should get just two of the results -
    # the lower clustering key in each.
    assert {(0,0,0,1), (1,0,0,5)} == set(cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 1'))

# GROUP BY the entire primary key doesn't really do anything, but is
# allowed
def test_group_by_entire_primary_key(cql, table1):
    assert [(0,0,0,1), (0,0,1,2), (0,1,0,3), (0,1,1,4)] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY p,c1,c2'))

# GROUP BY can only be given the primary key columns in order - anything
# else is an error. If some of the key columns are restricted by equality,
# they can be skipped in GROUP BY:
def test_group_bad_columns(cql, table1):
    # non-existent column:
    with pytest.raises(InvalidRequest, match='zzz'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY zzz')
    # Legal primary key prefixes are (p), (p,c1) and (p,c1,c2), already
    # tested above. Other combinations or orders like (c1) or (c1, p) are
    # illegal.
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY c1')
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY c1,c2')
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY p,c2')
    with pytest.raises(InvalidRequest, match='order'):
        cql.execute(f'SELECT p,c1,c2,v FROM {table1} GROUP BY c1,p')
    # Although we checked "GROUP BY c1" is not allowed, it becomes allowed
    # if p is restricted by equality
    assert [(0,0,0,1), (0,1,0,3)] == list(cql.execute(f'SELECT p,c1,c2,v FROM {table1} WHERE p=0 GROUP BY c1'))

### GROUP BY with aggregation. For example, when asking for count()
### we get a separate count for each group.
# NOTE: we have additional tests for combining aggregations and GROUP BY
# in test_aggregate.py - e.g., a test for #12477.
def test_group_by_count(cql, table1):
    assert {(0,4), (1,4)} == set(cql.execute(f'SELECT p,count(*) FROM {table1} GROUP BY p'))
    assert {(0,0,2), (0,1,2), (1,0,2), (1,1,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1'))
    assert {(0,0,0,1), (0,0,1,1), (0,1,0,1), (0,1,1,1), (1,0,0,1), (1,0,1,1), (1,1,0,1), (1,1,1,1)} == set(cql.execute(f'SELECT p,c1,c2,count(*) FROM {table1} GROUP BY p,c1,c2'))

# Adding a LIMIT should be honored.
# Reproduces #5361 - more results than the limit were generated (seems the
# limit was outright ignored).
def test_group_by_count_with_limit(cql, table1):
    results = list(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    for i in range(1,4):
        assert results[:i] == list(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1 LIMIT {i}'))

# Adding a PER PARTITION LIMIT should be honored
# Reproduces #5363 - more results than the limit were generated
@pytest.mark.xfail(reason="issue #5363")
def test_group_by_count_with_pplimit(cql, table1):
    # Without per-partition limit we get 4 results, 2 from each partition:
    assert {(0,0,2), (0,1,2), (1,0,2), (1,1,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1'))
    # With per-partition limit of 2, no change:
    assert {(0,0,2), (0,1,2), (1,0,2), (1,1,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 2'))
    # With per-partition limit of 1, we should get just two of the results -
    # the lower clustering key in each.
    # In issue #5363 we wrongly got here all four results (the PER PARTITION
    # LIMIT appears to have been ignored)
    assert {(0,0,2), (1,0,2)} == set(cql.execute(f'SELECT p,c1,count(*) FROM {table1} GROUP BY p,c1 PER PARTITION LIMIT 1'))

def test_group_by_sum(cql, table1):
    assert {(0,10), (1,26)} == set(cql.execute(f'SELECT p,sum(v) FROM {table1} GROUP BY p'))
    assert {(0,0,3), (0,1,7), (1,0,11), (1,1,15)} == set(cql.execute(f'SELECT p,c1,sum(v) FROM {table1} GROUP BY p,c1'))
    assert {(0,0,0,1), (0,0,1,2), (0,1,0,3), (0,1,1,4), (1,0,0,5), (1,0,1,6), (1,1,0,7), (1,1,1,8)} == set(cql.execute(f'SELECT p,c1,c2,sum(v) FROM {table1} GROUP BY p,c1,c2'))

# If selecting both an aggregation and real columns, we get both the result
# from *one* of the group rows (the first row by clustering order), and the
# aggregation from all of them.
def test_group_by_v_and_sum(cql, table1):
    assert {(0,1,10), (1,5,26)} == set(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p'))
    assert {(0,1,3), (0,3,7), (1,5,11), (1,7,15)} == set(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1'))
    assert {(0,1,1), (0,2,2), (0,3,3), (0,4,4), (1,5,5), (1,6,6), (1,7,7), (1,8,8)} == set(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1,c2'))

# Adding a LIMIT should be honored.
# Reproduces #5361 - more results than the limit were generated (seems the
# limit was outright ignored).
def test_group_by_v_and_sum_with_limit(cql, table1):
    results = list(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1'))
    assert len(results) == 4
    for i in range(1,4):
        assert results[:i] == list(cql.execute(f'SELECT p,v,sum(v) FROM {table1} GROUP BY p,c1 LIMIT {i}'))

# GROUP BY of a non-aggregated column qualified by ttl or writetime should work (#14715)
def test_group_by_non_aggregated_mutation_attribute_of_column(cql, table1):
    results = list(cql.execute(f'SELECT v, writetime(v), ttl(v) FROM {table1} GROUP BY p, c1'))
    assert len(results) == 4
    results = sorted(results, key=lambda row: row[0])
    assert [row[0] for row in results] == [1, 3, 5, 7]
    assert [row[1] for row in results] == [100000001, 100000003, 100000005, 100000007]
    assert all([[row[2] > 900000] for row in results])

# NOTE: we have tests for the combination of GROUP BY and SELECT DISTINCT
# in test_distinct.py (reproducing issue #12479).
