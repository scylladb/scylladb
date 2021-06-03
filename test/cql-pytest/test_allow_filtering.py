# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

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

@pytest.fixture(scope="session")
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
@pytest.mark.xfail(reason="issue #7608")
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
@pytest.mark.xfail(reason="issue #7608")
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
@pytest.mark.xfail(reason="issue #7608")
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
        if set(results) == set(everything):
            return
        time.sleep(0.1)
    pytest.fail('Timeout waiting for index to become up to date.')

@pytest.fixture(scope="session")
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
@pytest.fixture(scope="session")
def table3(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute("CREATE TABLE " + table +
        "(k1 int, k2 int, c1 int, c2 int, a int, b int, PRIMARY KEY ((k1,k2),c1,c2))")
    cql.execute("CREATE INDEX ON " + table + "(a)")
    cql.execute("CREATE INDEX ON " + table + "(b)")
    for i in range(0, 5):
        for j in range(0, 5):
            cql.execute("INSERT INTO {} (k1, k2, c1, c2, a, b) VALUES ({}, {}, {}, {}, {}, {})".format(
                table, i, j, i*10, j*10, i*100, j*100))
    everything = list(cql.execute('SELECT * FROM ' + table))
    wait_for_index(cql, table, 'b', everything)
    yield (table, everything)
    cql.execute("DROP TABLE " + table)

# In test_allow_filtering_indexed_a_and_k() above we noted that the
# combination of an indexed column and the partition key can be searched
# without filtering, and explained why. The same cannot be said for the
# combination of an indexed column and a clustering key - there may be
# many results matching the indexed column, and finding the ones matching
# the given clustering key value requires filtering.
def test_allow_filtering_indexed_a_and_c(cql, table3):
    check_af_mandatory(cql, table3, 'a=100 AND c1=10', lambda row: row.a==100 and row.c1==10)

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
# reproduces issue #5545.
def test_allow_filtering_pk_in(cql, table1):
    check_af_optional(cql, table1, 'k IN (1,2)', lambda row: row.k in {1,2})
@pytest.mark.xfail(reason="issue #5545: Scylla supports IN on indexed column, but only with ALLOW FILTERING")
def test_allow_filtering_index_in(cql, table2):
    check_af_optional(cql, table2, 'a IN (1,2)', lambda row: row.a in {1,2})

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
