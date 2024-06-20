# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for SELECT's LIMIT feature
#
# Note that LIMIT can be combined with many other features: GROUP BY,
# DISTINCT, CLUSTERING ORDER, aggregations, secondary indexes, paging,
# and more. So some tests involving a combination of these features are
# in other test files, not here:
# * LIMIT with GROUP BY (with and without aggregation) is tested in test_group_by.py
# * LIMIT with ORDER BY is in test_clustering_order.py
# * LIMIT with SELECT DISTINCT is in test_distinct.py
# * LIMIT with filtering is in test_secondary_index.py. Those tests check
#   that LIMIT should limit the number of post-filtering results, and run both
#   with and *without* a secondary index.
#
# As usual, all tests in this suite are run on a single node, so if we have
# bugs in the way that a coordinator aggregates results from multiple
# replicas, they are not tested here.
#
# All tests currently in this file only verify correctness - not performance:
# The tests don't check if the LIMIT is implemented efficiently (the replica
# stops reading after LIMIT rows) or in some inefficient manner (e.g., 1MB of
# data is read and then the excess data is dropped). We can consider later
# how to test performance aspects - perhaps using metrics.
#############################################################################

import pytest
from util import unique_key_int, new_test_table

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        yield table

# A basic test for "LIMIT" combined with no other feature (no filtering, no
# paging, no group by, etc.). There are two scenarios to check - a full table
# scan with a LIMIT, and a single-partition scan with a LIMIT.
def test_limit_full_table_scan(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key") as table:
        stmt = cql.prepare(f'INSERT INTO {table} (p) VALUES (?)')
        for i in range(10):
            cql.execute(stmt, [i])
        # We don't know how partitions get ordered, so let's scan the whole
        # table to get the expected order
        expected = list(cql.execute(f'SELECT p from {table}'))
        for limit in [1, 3, 7]:
            assert expected[0:limit] == list(cql.execute(f'SELECT p from {table} LIMIT {limit}'))

def test_limit_single_partition_scan(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES ({p}, ?)')
    for i in range(10):
        cql.execute(stmt, [i])
    for limit in [1, 3, 7]:
        assert list(range(limit)) == [x.c for x in cql.execute(f'SELECT c from {table1} WHERE p={p} LIMIT {limit}')]

# Check that *paging* is correctly supported with LIMIT - paging should continue
# until reaching LIMIT number of total results (and not forget how much we
# still need to read until the limit).
def test_limit_single_partition_scan_with_paging(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES ({p}, ?)')
    for i in range(10):
        cql.execute(stmt, [i])
    # If we read with fetch_size=3 and LIMIT=7, we should read a first page
    # with 3 results, and then two more pages, until a total of 7 results.
    # If fetch_size=8 and LIMIT=7, we should read just 7 results in the
    # first page.
    limit = 7
    s = cql.prepare(f'SELECT c from {table1} WHERE p={p} LIMIT {limit}')
    for page_size in range(10):
        s.fetch_size = page_size
        results = cql.execute(s)
        if page_size > 0:
            assert len(results.current_rows) == min(page_size, limit)
        else:
            # page_size=0 means use some large default, so we expect all
            # "limit" results in the first page
            assert len(results.current_rows) == limit
        # Finish reading all the pages and expect the full "limit":
        assert list(range(limit)) == [x.c for x in cql.execute(f'SELECT c from {table1} WHERE p={p} LIMIT {limit}')]

# Check the combination of LIMIT and PER PARTITION LIMIT. It should only
# return the first PER PARTITION LIMIT rows from each partition, up to
# a total of LIMIT rows overall. Check with and without paging.
def test_limit_and_per_partition_limit(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, primary key (p,c)") as table:
        stmt = cql.prepare(f'INSERT INTO {table} (p,c) VALUES (?, ?)')
        # This test insert 9 items (p,c), 0<=p,c<3, and then reads them with
        # PER PARTITION LIMIT 2 and different LIMITs and page sizes.
        for i in range(3):
            for j in range(3):
                cql.execute(stmt, [i, j])
        expected = [(x.p, x.c) for x in cql.execute(f'SELECT p,c from {table}')]
        # check the results with PER PARTITION LIMIT 2, without a LIMIT:
        expected_ppl2 = [e for i, e in enumerate(expected) if i % 3 != 2]
        assert expected_ppl2 == [(x.p, x.c) for x in cql.execute(f'SELECT p,c from {table} PER PARTITION LIMIT 2')]
        # Now check with different LIMITs:
        for limit in [1,2,3,4,5,7,20]:
            s = cql.prepare(f'SELECT p,c from {table} PER PARTITION LIMIT 2 LIMIT {limit}')
            for page_size in [0, 1, 2]:
                s.fetch_size = page_size
                results = cql.execute(s)
                if page_size > 0:
                    assert len(results.current_rows) == min(page_size, limit, len(expected_ppl2))
                else:
                    # page_size=0 means use some large default, so first page will
                    # have all results
                    assert len(results.current_rows) == min(limit, len(expected_ppl2))
                # Finish reading all the pages and expect the first "limit" rows of expected_ppl2
                assert expected_ppl2[0:limit] == [(x.p, x.c) for x in results]

# Check the combination of LIMIT and IN. The LIMIT should constrain the total number
# of results, from either partition. Check with and without paging.
def test_limit_and_in(cql, table1):
    p1 = unique_key_int()
    p2 = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES (?, ?)')
    for i in range(5):
        cql.execute(stmt, [p1,i])
        cql.execute(stmt, [p2,i])
    all_results = list(cql.execute(f'SELECT p,c FROM {table1} WHERE p IN ({p1},{p2})'))
    for limit in [1,2,6,12]:
        s = cql.prepare(f'SELECT p,c FROM {table1} WHERE p IN ({p1},{p2}) LIMIT {limit}')
        for page_size in [0, 1, 2, 5, 6]:
            s.fetch_size = page_size
            results = cql.execute(s)
            if page_size > 0:
                assert len(results.current_rows) == min(page_size, limit, len(all_results))
            else:
                assert len(results.current_rows) == min(limit, len(all_results))
            # Finish reading all the pages and expect the first "limit" rows of expected_ppl2
            assert all_results[0:limit] == list(results)

# Check the combination of LIMIT and tombstones (deleted rows).
# LIMIT should always limit the number of actual results, so tombstones
# aren't counted (tombstones can cause a smaller *page* of results, but this
# isn't the same as LIMIT).
def test_limit_single_partition_scan_with_tombstones(cql, table1):
    p = unique_key_int()
    stmt = cql.prepare(f'INSERT INTO {table1} (p, c) VALUES ({p}, ?)')
    dstmt = cql.prepare(f'DELETE FROM {table1} WHERE p={p} AND c=?')
    for i in range(10):
        cql.execute(stmt, [i])
        if i%2 == 0:
            cql.execute(dstmt, [i])
    assert [1,3,5,7] == [x.c for x in cql.execute(f'SELECT c from {table1} WHERE p={p} LIMIT 4')]
