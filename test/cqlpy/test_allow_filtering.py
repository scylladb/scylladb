# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the "ALLOW FILTERING" specification on SELECT requests.
# ALLOW FILTERING is required in specific cases, but not required
# (but allowed) in other cases.
#
# Our documentation, https://docs.scylladb.com/getting-started/dml/, states
# that queries that do not need ALLOW FILTERING "have predictable
# performance in the sense that they will execute in a time that is
# proportional to the amount of data returned by the query (which can be
# controlled through LIMIT).".
#
# Note that we have several "xfail"ing tests below. They fail because in
# those cases Scylla does not live up to its own documented definition -
# either we don't require ALLOW FILTERING on a query that may produce only
# a few results and yet need to go over millions of partitions to produce
# them, or the opposite problem: a query which can be done in time
# proportional to the result size wrongly requires ALLOW FILTERING.
#
# Most of the tests below go beyond just checking whether ALLOW FILTERING
# is required: The tests also check that when the query is allowed, its
# results are the correct filtered subset of the entire content of the table.

import pytest
import re
import time

from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException, ReadFailure

from util import unique_name, new_test_table

# check_af_optional() and check_af_mandatory() are utility functions for
# checking that ALLOW FILTERING is optional or mandatory on a SELECT on a
# certain table with a given WHERE restriction.
# Moreover, these functions also test that result of the request, when
# allowed, is the correct result. We check this by comparing the result of
# the request with what we will get by filtering in Python the entire
# contents of the table ('everything') with the given 'filt' function.

def check_af_optional(cql, table_and_everything, where, filt):
    (table, everything) = table_and_everything
    # Check that the query is allowed even without ALLOW FILTERING
    # (and returns the correct results):
    results = list(cql.execute(f'SELECT * FROM {table} WHERE {where}'))
    if filt:
        expected_results = list(filter(filt, everything))
        assert results == expected_results
    # Check that adding ALLOW FILTERING is unnecessary, but allowed:
    results = list(cql.execute(f'SELECT * FROM {table} WHERE {where} ALLOW FILTERING'))
    if filt:
        assert results == expected_results

def check_af_mandatory(cql, table_and_everything, where, filt):
    (table, everything) = table_and_everything
    # Check that without ALLOW FILTERING, this query is forbidden.
    # When ALLOW FILTERING is required, we always get an InvalidRequest
    # error, but unfortunately the error message is different in different
    # cases. There is one message for the general case and one for the
    # specific case of a partition-key restriction - both include the
    # string "allow filtering" with different capitalization, and there is
    # a third error message when attempting to restrict a non-prefix
    # clustering key - unfortunately this message doesn't mention "ALLOW
    # FILTERING" at all, although it should because the error goes away
    # when ALLOW FILTERING is specified.
    with pytest.raises(InvalidRequest, match=re.compile('allow filtering|cannot be restricted', re.IGNORECASE)):
        cql.execute(f'SELECT * FROM {table} WHERE {where}')
    # Check that with ALLOW FILTERING, the query is allowed, and returns
    # the correct results:
    results = list(cql.execute(f'SELECT * FROM {table} WHERE {where} ALLOW FILTERING'))
    if filt:
        expected_results = list(filter(filt, everything))
        assert results == expected_results

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(k int, c int, v int, PRIMARY KEY (k,c))")
    for i in range(0, 3):
        for j in range(0, 3):
            cql.execute(f'INSERT INTO {table} (k, c, v) VALUES ({i}, {j}, {j})')
    everything = list(cql.execute('SELECT * FROM ' + table))
    yield (table, everything)
    cql.execute("DROP TABLE " + table)

# Reading an entire partition, or a contiguous "slice" of a partition, is
# allowed without ALLOW FILTERING. Adding an unnecessary ALLOW FILTERING
# is also allowed.
def test_allow_filtering_partition_slice(cql, table1):
    check_af_optional(cql, table1, 'k=1', lambda row: row.k==1)
    check_af_optional(cql, table1, 'k=1 AND c>2', lambda row: row.k==1 and row.c>2)
    check_af_optional(cql, table1, 'k=1 AND c<2', lambda row: row.k==1 and row.c<2)
    check_af_optional(cql, table1, 'k=1 AND c=2', lambda row: row.k==1 and row.c==2)
    check_af_optional(cql, table1, 'k=1 AND c>=2 AND c<=4', lambda row: row.k==1 and row.c>=2 and row.c<=4)

# Although as the above test showed that a partition slice does not require
# filtering, if we add another restriction like 'v=2' the filtering *is*
# required: The point is that the slice may have many rows, out of which
# only a small number might match 'v=2'.
# The above is true only for potentially-long slices. The single-row slice
# 'k=1 and c=2' is different, and will be tested in the following test
# test_allow_filtering_single_row().
def test_allow_filtering_partition_slice_and_restriction(cql, table1):
    check_af_mandatory(cql, table1, 'k=1 AND v=2', lambda row: row.k==1 and row.v==2)
    check_af_mandatory(cql, table1, 'k=1 AND c>2 AND v=2', lambda row: row.k==1 and row.c>2 and row.v==2)
    check_af_mandatory(cql, table1, 'k=1 AND c<2 AND v=2', lambda row: row.k==1 and row.c<2 and row.v==2)
    check_af_mandatory(cql, table1, 'k=1 AND c>=2 AND c<=4 AND v=2', lambda row: row.k==1 and row.c>=2 and row.c<=4 and row.v==2)

# Reading a clustering single row 'k=1 AND c=2' always takes a O(1) amount
# of work and returns one or zero results. Adding another restriction
# like 'v=2' doesn't change any of the above, so doesn't require filtering.
# Reproduces #7964 on Scylla, and also wrong on Cassandra so marked
# cassandra_bug.
@pytest.mark.xfail(reason="#7964")
def test_allow_filtering_single_row(cql, table1, cassandra_bug):
    check_af_optional(cql, table1, 'k=1 AND c=2', lambda row: row.k==1 and row.c==2)
    # Reproduces #7964, as ALLOW FILTERING is considered mandatory, not optional
    check_af_optional(cql, table1, 'k=1 AND c=2 AND v=2', lambda row: row.k==1 and row.c==2 and row.v==2)

# A scan of the whole table or of a whole partition looking for one particular
# regular column value requires filtering.
def test_allow_filtering_regular_column(cql, table1):
    check_af_mandatory(cql, table1, 'v=2', lambda row: row.v==2)
    check_af_mandatory(cql, table1, 'k=1 AND v=2', lambda row: row.k==1 and row.v==2)

# A scan of the whole table looking for one particular *clustering* column
# value requires filtering: Such a query may return just a few or even no
# matches, but still needs to go over all the partitions - and for each
# partition it needs to inspect the promoted index and in the worst case,
# also read a chunk of rows to look for the matching clustering-column value.
# Reproduces issue #7608.
def test_allow_filtering_clustering_key(cql, table1):
    check_af_mandatory(cql, table1, 'c=2', lambda row: row.c==2)
    check_af_mandatory(cql, table1, 'c>2', lambda row: row.c>2)
    # But looking for a particular clustering column value inside a specific
    # partition does not require filtering.
    check_af_optional(cql, table1, 'c=2 AND k = 1', lambda row: row.c==2 and row.k==1)

# Same as the above test_allow_filtering_clustering_key, except that instead
# of scanning all the partitions, we limit the scan half of them using a
# token(k) > 123 restriction. If in the previous test filtering was mandatory,
# so should it be in this one. The same is true also for finite token
# ranges - because although they are finite, they may still contain a lot
# of partitions.
# Reproduces issue #7608.
def test_allow_filtering_clustering_key_token_range(cql, table1):
    # TODO: the current implementation of check_af_mandatory() doesn't know
    # how to request or compare results with the token, so we pass None to
    # so it only checks that ALLOW FILTERING is mandatory.
    check_af_mandatory(cql, table1, 'c=2 AND token(k) > 123', None)
    check_af_mandatory(cql, table1, 'c=2 AND token(k) > 123 AND token(k) < 200', None)

# In the above test we noted that filtering by clustering key in a
# potentially large subset of the partitions (a token range) also needs
# ALLOW FILTERING. In this test we check what happens if the query is
# narrowed to just *one* specific token - 'c=2 AND token(k) = 123'.
# In this case too, in theory, just one token may actually match many
# different partitions, so Cassandra also asks for ALLOW FILTERING in
# this case. Note that 'c=2 AND k = 123' (without the "token"), which
# is guaranteed to match just one partition, doesn't need ALLOW FILTERING.
# Reproduces issue #7608.
def test_allow_filtering_clustering_key_token_specific(cql, table1):
    check_af_mandatory(cql, table1, 'c=2 AND token(k) = 123', None)
    check_af_optional(cql, table1, 'c=2 AND k = 123', None)

# A restriction on the partition key other than equality requires filtering.
def test_allow_filtering_partition_key(cql, table1):
    check_af_optional(cql, table1, 'k=2', lambda row: row.k==2)
    check_af_mandatory(cql, table1, 'k>2', lambda row: row.k>2)

# A utility function for waiting for a secondary index to become up-
# up-to-date (this may not be immediate).
def wait_for_index(cql, table, column, everything):
    # It would be nice if we had a better way to check when the index is
    # up to date, but this ugly approach should work too. It's done only
    # once or twice anyway, and our tables are small.
    start_time = time.time()
    while time.time() < start_time + 10:
        column_values = {getattr(row, column) for row in everything}
        results = []
        for v in column_values:
            results.extend(list(cql.execute(f'SELECT * FROM {table} WHERE {column}={v}')))

        if sorted(results) == sorted(everything):
            return
        
        time.sleep(0.1)

    pytest.fail('Timeout waiting for index to become up to date.')

# Similar to wait_for_index(), just for a local secondary index
def wait_for_local_index(cql, table, pk, column, everything):
    start_time = time.time()
    while time.time() < start_time + 10:
        column_values = {(getattr(row, pk), getattr(row, column)) for row in everything}
        results = []
        for p, v in column_values:
            results.extend(list(cql.execute(f'SELECT * FROM {table} WHERE {pk}={p} AND {column}={v}')))
        if sorted(results) == sorted(everything):
            return
        time.sleep(0.1)
    pytest.fail('Timeout waiting for index to become up to date.')

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(k int, a int, b int, PRIMARY KEY (k))")
    cql.execute("CREATE INDEX ON " + table + "(a)")
    for i in range(0, 5):
        cql.execute("INSERT INTO {} (k, a, b) VALUES ({}, {}, {})".format(
                table, i, i*10, i*100))
    everything = list(cql.execute('SELECT * FROM ' + table))
    wait_for_index(cql, table, 'a', everything)
    yield (table, everything)
    cql.execute("DROP TABLE " + table)

# When an index is available, beyond the normal ability to efficiently
# (without ALLOW FILTERING) retrieve a partition or a partition slice,
# we can retrieve a particular value of the indexed column "a" without
# filtering.
def test_allow_filtering_indexed_no_filtering(cql, table2):
    check_af_optional(cql, table2, 'k=1', lambda row: row.k==1)
    check_af_optional(cql, table2, 'a=20', lambda row: row.a==20)

# When an index is available, we still need ALLOW FILTERING to filter
# on an additional regular column.
def test_allow_filtering_indexed_filtering_required(cql, table2):
    check_af_mandatory(cql, table2, 'a=20 AND b=200', lambda row: row.a==20 and row.b==200)

# Searching for the intersection of indexed value *and* a partition key
# (a=20 and k=1) can be done without filtering: The search for a=20 results
# in a partition, whose first clustering key is the partition key, so we
# can skip to it without filtering.
def test_allow_filtering_indexed_a_and_k(cql, table2):
    check_af_optional(cql, table2, 'a=20 AND k=1', lambda row: row.a==20 and row.k==1)


# table3 is an even more elaborate table with several partition key columns,
# clustering key columns, and indexed columns of different types, to allow
# us to check even more esoteric cases.
@pytest.fixture(scope="module")
def table3(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(k1 int, k2 int, c1 int, c2 int, a int, b int, s int static, PRIMARY KEY ((k1,k2),c1,c2))")
    cql.execute("CREATE INDEX ON " + table + "(s)")
    cql.execute("CREATE INDEX ON " + table + "(a)")
    cql.execute("CREATE INDEX ON " + table + "(b)")
    for i in range(0, 5):
        for j in range(0, 5):
            cql.execute("INSERT INTO {} (k1, k2, c1, c2, a, b, s) VALUES ({}, {}, {}, {}, {}, {}, {})".format(
                table, i, j, i*10, j*10, i*100, j*100, i+j))
    everything = list(cql.execute('SELECT * FROM ' + table))
    wait_for_index(cql, table, 'a', everything)
    wait_for_index(cql, table, 'b', everything)
    wait_for_index(cql, table, 's', everything)
    yield (table, everything)
    cql.execute("DROP TABLE " + table)

def test_allow_filtering_multi_column(cql, table3):
    """Multi-column restrictions are just like other clustering restrictions"""
    check_af_mandatory(cql, table3, '(c1,c2)=(10,10)', lambda row: row.c1==10 and row.c2==10)
    check_af_optional(cql, table3, '(c1,c2)=(10,10) and k1=1 and k2=1',
            lambda row: row.c1==10 and row.c2==10 and row.k1==1 and row.k2==1)

# In test_allow_filtering_indexed_a_and_k() above we noted that the
# combination of an indexed column and the partition key can be searched
# without filtering, and explained why. The same cannot be said for the
# combination of an indexed column and a clustering key - there may be
# many results matching the indexed column, and finding the ones matching
# the given clustering key value requires filtering.
def test_allow_filtering_indexed_a_and_c(cql, table3):
    check_af_mandatory(cql, table3, 'a=100 AND c1=10', lambda row: row.a==100 and row.c1==10)

# Exactly the same for an indexed static column: s=5 may match many rows,
# and c1=10 may restrict the returned list to much fewer.
# (See also discussion in #12828)
def test_allow_filtering_indexed_s_and_c(cql, table3):
    check_af_mandatory(cql, table3, 's=5 AND c1=10', lambda row: row.s==5 and row.c1==10)

# Similarly, trying two combine two different indexes requires filtering.
# Scylla does not have efficient intersections of two indexes.
def test_allow_filtering_indexed_two_indexes(cql, table3):
    check_af_mandatory(cql, table3, 'a=100 AND b=100', lambda row: row.a==100 and row.b==100)

# Test that queries involving key columns still requires filtering if
# the searched columns are not a full partition key, or not a prefix
# of the clustering key.
def test_allow_filtering_prefix(cql, table3):
    check_af_mandatory(cql, table3, 'k1=1', lambda row: row.k1==1)
    check_af_mandatory(cql, table3, 'k2=1', lambda row: row.k2==1)
    check_af_optional(cql, table3, 'k1=1 AND k2=1', lambda row: row.k1==1 and row.k2 ==1)
    check_af_optional(cql, table3, 'k1=1 AND k2=1 AND c1=10', lambda row: row.k1==1 and row.k2==1 and row.c1==10)
    check_af_mandatory(cql, table3, 'k1=1 AND k2=1 AND c2=10', lambda row: row.k1==1 and row.k2==1 and row.c2==10)
    check_af_optional(cql, table3, 'k1=1 AND k2=1 AND c1=10 AND c2=10', lambda row: row.k1==1 and row.k2==1 and row.c1==10 and row.c2==10)

# Just like "k=1" (for partition key) and "a=1" (for indexed column) does
# not require filtering, neither should "k in (1,2)" or "a in (1,2)" -
# which should just result in the union of the two result sets.
# Scylla and Cassandra both do this correctly for a partition key, but
# fail for different reasons for an indexed column: Cassandra states that
# "IN predicates on non-primary-key columns (a) is not yet supported" and
# doesn't work at all (not without ALLOW FILTERING and not with it); Scylla
# does support this use case with ALLOW FILTERING, but refuses to do it
# without ALLOW FILTERING. The test test_allow_filtering_index_in()
# reproduces issue #5545 / #13533
def test_allow_filtering_pk_in(cql, table1):
    check_af_optional(cql, table1, 'k IN (1,2)', lambda row: row.k in {1,2})
@pytest.mark.xfail(reason="issue #5545, #13533: Scylla supports IN on indexed column, but only with ALLOW FILTERING")
def test_allow_filtering_index_in(cql, table2):
    check_af_optional(cql, table2, 'a IN (1,2)', lambda row: row.a in {1,2})

# Exactly the same bug #13533 as tested above in
# test_allow_filtering_index_in, also exists for *local* secondary
# indexes, and reproduced by the following test. Local secondary indexes
# are a Scylla-only feature, so the test is marked as scylla_only.
@pytest.fixture(scope="module")
def table2local(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(k int, a int, b int, PRIMARY KEY (k))")
    cql.execute("CREATE INDEX ON " + table + "((k), a)")
    for i in range(0, 5):
        cql.execute("INSERT INTO {} (k, a, b) VALUES ({}, {}, {})".format(
                table, i//2, i*10, i*100))
    everything = list(cql.execute('SELECT * FROM ' + table))
    wait_for_local_index(cql, table, 'k', 'a', everything)
    yield (table, everything)
    cql.execute("DROP TABLE " + table)
@pytest.mark.xfail(reason="issue #13533: Scylla supports IN on indexed column, but only with ALLOW FILTERING")
def test_allow_filtering_local_index_in(cql, table2local, scylla_only):
    check_af_optional(cql, table2local, 'k = 0 AND a IN (1,2)', lambda row: row.a in {1,2})

# The following test Reproduces bug #7888 in CONTAINS/CONTAINS KEY relations
# on frozen collection clustering columns when the query is restricted to a
# single partition. Analogous to CASSANDRA-8302, and also reproduced by
# Cassandra's more elaborate test for this issue,
# cassandra_tests/validation/entities/frozen_collections_test.py::testClusteringColumnFiltering
def test_contains_frozen_collection_ck(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b frozen<map<int, int>>, c int, PRIMARY KEY (a,b,c)") as table:
        # The CREATE INDEX for c is necessary to reproduce this bug.
        # Everything works without it.
        cql.execute(f"CREATE INDEX ON {table} (c)")
        cql.execute("INSERT INTO " + table + " (a, b, c) VALUES (0, {0: 0, 1: 1}, 0)")
        # The "a=0" below is necessary to reproduce this bug.
        assert 1 == len(list(cql.execute(
            "SELECT * FROM " + table + " WHERE a=0 AND c=0 AND b CONTAINS 0 ALLOW FILTERING")))
        assert 1 == len(list(cql.execute(
            "SELECT * FROM " + table + " WHERE a=0 AND c=0 AND b CONTAINS KEY 0 ALLOW FILTERING")))

# table4 contains example table from issue #8991
@pytest.fixture(scope="module")
def table4(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k int, c1 int, c2 int, PRIMARY KEY (k,c1,c2))")
    cql.execute(f"CREATE INDEX ON {table} (c2)")
    cql.execute(f"INSERT INTO {table} (k, c1, c2) VALUES (0, 0, 1)")
    cql.execute(f"INSERT INTO {table} (k, c1, c2) VALUES (0, 1, 1)")

    everything = list(cql.execute(f"SELECT * FROM {table}"))
    wait_for_index(cql, table, 'c2', everything)
    yield (table, everything)
    cql.execute(f"DROP TABLE {table}")

# Selecting from indexed table using only clustering key should require filtering
# Variation of #7608 found in #8991
def test_select_indexed_cluster(cql, table4):
    check_af_mandatory(cql, table4, 'c1 = 1 AND c2 = 1', lambda row: row.c1 == 1 and row.c2 == 1)

# table5 contains an indexed table with 3 clustering columns.
# used to test correct filtering of rows fetched from an index table.
@pytest.fixture(scope="module")
def table5(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int, c1 frozen<list<int>>, c2 frozen<list<int>>, c3 int, PRIMARY KEY (p,c1,c2,c3))")
    cql.execute(f"CREATE INDEX ON {table} (c3)")
    cql.execute(f"INSERT INTO {table} (p, c1, c2, c3) VALUES (0, [1], [2], 0)")
    cql.execute(f"INSERT INTO {table} (p, c1, c2, c3) VALUES (0, [2], [2], 0)")
    cql.execute(f"INSERT INTO {table} (p, c1, c2, c3) VALUES (0, [1], [3], 0)")
    cql.execute(f"INSERT INTO {table} (p, c1, c2, c3) VALUES (0, [1], [2], 1)")

    everything = list(cql.execute(f"SELECT * FROM {table}"))
    wait_for_index(cql, table, 'c3', everything)
    yield (table, everything)
    cql.execute(f"DROP TABLE {table}")

# Test that implementation of filtering for indexes works ok.
# Current implementation is a bit conservative - it might sometimes state
# that filtering is needed when it isn't actually required, but at least it's safe.
def test_select_indexed_cluster_three_keys(cql, table5):
    def check_good_row(row):
        return row.p == 0 and row.c1 == [1] and row.c2 == [2] and row.c3 == 0
    
    check_af_optional(cql, table5, "c3 = 0", lambda r : r.c3 == 0)
    check_af_mandatory(cql, table5, "c1 = [1] AND c2 = [2] AND c3 = 0", check_good_row)
    check_af_mandatory(cql, table5, "p = 0 AND c1 CONTAINS 1 AND c3 = 0", lambda r : r.p == 0 and r.c1 == [1] and r.c3 == 0)
    check_af_mandatory(cql, table5, "p = 0 AND c1 = [1] AND c2 CONTAINS 2 AND c3 = 0", check_good_row)

    # Doesn't use an index - shouldn't be affected
    check_af_optional(cql, table5, "p = 0 AND c1 = [1] AND c2 = [2] AND c3 = 0", check_good_row)

# Here are the cases where current implementation of need_filtering() fails
# By coincidence they also fail on cassandra, it looks like cassandra is buggy
@pytest.mark.xfail(reason="Too conservative need_filtering() implementation")
def test_select_indexed_cluster_three_keys_conservative(cql, table5, cassandra_bug):
    def check_good_row(row):
        return row.p == 0 and row.c1 == [1] and row.c3 == 0

    # Don't require filtering, but for now we report they do
    check_af_optional(cql, table5, "p = 0 AND c1 = [1] AND c3 = 0", check_good_row)
    check_af_optional(cql, table5, "p = 0 AND c1 = [1] AND c2 < [3] AND c3 = 0", lambda r : check_good_row(r) and r.c2 < [3])

# This test demonstrates a loose end after issue #9085 was fixed by PR #9122.
# The fix ensured correct results - but not correct ALLOW FILTERING need.
# In issue #9085, we have a query with a restriction on an indexed column plus
# the base table's partition key *and* a multi-column restriction on the
# compound clustering key. The incorrect results happened because the multi-
# column restriction was being ignored. The fix in PR #9122 was to apply the
# multi-column restriction as a "filtering" step. But this means we now
# require ALLOW FILTERING in this case, even when the query has a full
# prefix of the index table's clustering key.
# This test demonstrates how after PR #9122 we wrongly require ALLOW FILTERING
# in one such case.
# This test is basically the same as test_multi_column_with_regular_index in
# test_secondary_index.py, but it focuses on the correctness of the ALLOW
# FILTERING requirement, not just on the correctness of the results.
@pytest.mark.xfail(reason="PR #9122 loose end")
def test_allow_filtering_multi_column_and_index(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c1 int, c2 int, r int, primary key(p,c1,c2)') as table:
        cql.execute(f'CREATE INDEX ON {table}(r)')
        cql.execute(f'INSERT INTO {table}(p, c1, c2, r) VALUES (1, 1, 1, 0)')
        cql.execute(f'INSERT INTO {table}(p, c1, c2, r) VALUES (1, 1, 2, 1)')
        cql.execute(f'INSERT INTO {table}(p, c1, c2, r) VALUES (1, 2, 1, 0)')
        cql.execute(f'INSERT INTO {table}(p, c1, c2, r) VALUES (2, 2, -1, 0)')
        everything = list(cql.execute(f"SELECT * FROM {table}"))
        wait_for_index(cql, table, 'r', everything)
        # If the base table's partition key (p) is missing in the query, it's
        # not a full prefix of the index table's clustering key, so filtering
        # *is* needed:
        check_af_mandatory(cql, (table, everything),
            "(c1,c2)<(2,0) AND r = 0",
            lambda r : r.r == 0 and (r.c1 < 2 or (r.c1 == 2 and r.c2 < 0)))
        # But if the base table's partition key is in the query, along with
        # the clustering key, then it's a full prefix of the index's
        # clustering key - so filtering is not needed. PR #9122 fixed the
        # correctness of this query, but left ALLOW FILTERING mandatory so
        # the following test failed:
        check_af_optional(cql, (table, everything),
            "p=1 AND (c1,c2)<(2,0) AND r = 0",
            lambda r : r.p == 1 and r.r == 0 and (r.c1 < 2 or (r.c1 == 2 and r.c2 < 0)))

# In test_allow_filtering_clustering_key above we checked that a scan of the
# whole table looking for one particular *clustering* column value requires
# filtering: Such a query may return just a few or even no matches, but still
# needs to go over all the partitions. Here we do exactly the same but
# instead of the restriction c=2 we use multi-column syntax (c)=(2) - which
# should be the same (see discussion in issue #13250).
def test_allow_filtering_clustering_key_multicolumn_syntax(cql, table1):
    check_af_mandatory(cql, table1, '(c)=(2)', lambda row: row.c==2)

# Moreover, if we have multiple clustering key columns, c1 and c2,
# (c2)=(10) should be allowed just like c2=10 (and require filtering
# just like it) - we shouldn't complain that c1 is missing. Reproduces #13250.
@pytest.mark.xfail(reason="issue #13250")
def test_allow_filtering_compound_clustering_key_multicolumn_syntax(cql, table3):
    check_af_mandatory(cql, table3, 'c1=10', lambda row: row.c1==10)
    check_af_mandatory(cql, table3, '(c1)=(10)', lambda row: row.c1==10)
    check_af_mandatory(cql, table3, 'c2=10', lambda row: row.c2==10)
    # Reproduces #13250:
    check_af_mandatory(cql, table3, '(c2)=(10)', lambda row: row.c2==10)

# Intersecting two indexes requires ALLOW FILTERING, because no matter how
# efficient we implement index intersection (and Scylla doesn't...), there
# is always a worst case like the following:
#   * Imagine we have a million rows, we set x=1 on the even rows, y=1 on odd.
#   * We create a secondary index on x and y.
#   * We select WHERE x=1 AND y=1.
# This query needs to process at least half a million rows before returning
# no matches. This is ALLOW FILTERING par excellence.
def test_allow_filtering_index_intersection(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, x int, y int, primary key(p)') as table:
        cql.execute(f'CREATE INDEX ON {table}(x)')
        cql.execute(f'CREATE INDEX ON {table}(y)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (0, 0, 0)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (1, 0, 1)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (2, 1, 0)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (3, 1, 1)')
        everything = list(cql.execute(f"SELECT * FROM {table}"))
        wait_for_index(cql, table, 'x', everything)
        wait_for_index(cql, table, 'y', everything)
        # If we search on just one index, ALLOW FILTERING is not needed:
        check_af_optional(cql, (table, everything),
            "x = 1",
            lambda r : r.x == 1)
        check_af_optional(cql, (table, everything),
            "y = 1",
            lambda r : r.y == 1)
        # But intersecting two indexes, ALLOW FILTERING is required:
        check_af_mandatory(cql, (table, everything),
            "x = 1 AND y = 1",
            lambda r : r.x == 1 and r.y == 1)

# Exactly the same test as the above, but using the "SAI" indexer, which
# reproduces a regression in Cassandra 5's SAI compared to their classic
# index (tested in the above test) - reported as CASSANDRA-19795.
# Since Scylla doesn't support SAI, we skip the test on Scylla (and also
# on older Cassandra).
def test_allow_filtering_index_intersection_sai(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int, x int, y int, primary key(p)') as table:
        try:
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(x) USING 'SAI'")
            cql.execute(f"CREATE CUSTOM INDEX ON {table}(y) USING 'SAI'")
        except (InvalidRequest, ConfigurationException):
            pytest.skip('SAI test skipped, SAI not supported')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (0, 0, 0)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (1, 0, 1)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (2, 1, 0)')
        cql.execute(f'INSERT INTO {table}(p, x, y) VALUES (3, 1, 1)')
        everything = list(cql.execute(f"SELECT * FROM {table}"))
        wait_for_index(cql, table, 'x', everything)
        wait_for_index(cql, table, 'y', everything)
        check_af_optional(cql, (table, everything),
            "x = 1",
            lambda r : r.x == 1)
        check_af_optional(cql, (table, everything),
            "y = 1",
            lambda r : r.y == 1)
        check_af_mandatory(cql, (table, everything),
            "x = 1 AND y = 1",
            lambda r : r.x == 1 and r.y == 1)
