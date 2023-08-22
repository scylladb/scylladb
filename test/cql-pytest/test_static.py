# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for static-column support in Scylla.
# Note that we have many more tests for static columns in other test
# frameworks (C++, cql, dtest) but the benefit of these tests is that they
# allow running tests with debatable behavior on Cassandra as well.
#############################################################################

import pytest

from util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    schema='p int, c int, r int, s int static, PRIMARY KEY(p, c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        yield table

# Test what happens when we SELECT a partition which has a static column
# set but no clustering row, but the static column is *not* selected.
# Reproduces issue #10091.
#
# Marking this issue "xfail" because Scylla and Cassandra differ in behavior
# and we have no good explanation why Scylla's behavior is more correct than
# Cassandra. If we later decide that we consider *both* behaviors equally
# correct, we can change the test to accept both and make it pass.
@pytest.mark.xfail(reason="issue #10091")
def test_static_not_selected(cql, table1):
    p = unique_key_int()
    # The partition p doesn't exist, so the following select yields nothing:
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == []
    # Insert just the static column, and no clustering row:
    cql.execute(f'INSERT INTO {table1} (p, s) values ({p}, 1)')
    # If we select all the columns, including the static column s, SELECTing
    # the partition gives us one "row" with the static column set and all
    # other columns set to null - otherwise the static column cannot be
    # returned.
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p,None,1,None)]
    # But what happens if we SELECT just regular (non-static) columns?
    # Should the SELECT return nothing (since there is no clustering row),
    # or return one "row" with null columns (basically just like in the
    # previous SELECT, just intersected with the desired column)?
    # Currently, Cassandra does the former, Scylla does the latter,
    # so the following assert fails on Scylla:
    assert list(cql.execute(f'SELECT r FROM {table1} WHERE p={p}')) == []

# Verify that if a partition has a static column set, reading an *existing*
# clustering row will return it, but reading a *non-existing* row will not
# return anything - not even the static column. 
#
# Contrast this with issue #10081 (test_lwt.py::test_lwt_missing_row_with_static)
# where we suggested that in an UPDATE with IF condition (LWT), the static
# columns should be readable in the condition expression, whether or not the
# given row exists.
def test_missing_row_with_static(cql, table1):
    p = unique_key_int()
    # Insert into partition p just static column and once clustering row c=2
    cql.execute(f'INSERT INTO {table1}(p, s, c, r) values ({p}, 1, 2, 3)')
    # If we SELECT row c=2, we get it and the static column:
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND c=2')) == [(p, 1, 2, 3)]
    # If we SELECT row c=1 (which doesn't exist), we get nothing - not even
    # the static column
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND c=1')) == []

# Verify that if a partition has a static column set, filtering it (with
# ALLOW FILTERING) based on regular columns will need to find clustering
# rows which actually match this filter - and can't return only a static
# row (which doesn't match the filter). This is similar to the previous test
# (test_missing_row_with_static) except that the filter here is on a regular
# column, not a clustering column.
def test_filter_with_static(cql, table1):
    p = unique_key_int()
    # Insert into partition p just static column and one clustering row
    # with its regular column r set to 3:
    cql.execute(f'INSERT INTO {table1}(p, s, c, r) values ({p}, 1, 2, 3)')
    # If we filter for r=3, we expect to get the clustering row and the
    # static column:
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND r=3 ALLOW FILTERING')) == [(p, 1, 2, 3)]
    # If we filter for r=4, we expect to get nothing - not even the static
    # column. Getting back just the static row, with r=null, would be wrong
    # because it does *not* match the filter r=4.
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND r=4 ALLOW FILTERING')) == []

# Reproduce issue #10357. The query is identical to the one in the above
# test (test_filter_with_static), but here the partition has only a static
# column value, and no clustering row at all. We expect to get back nothing
# when using a filtering on a regular column.
@pytest.mark.xfail(reason="issue #10357")
def test_filter_with_only_static(cql, table1):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p, s) values ({p}, 1)')
    # There are no clustering rows or r column set for anything, so filtering
    # for r=4 should return nothing. But issue #10357 caused Scylla to wrongly
    # return a row with only the static value - which doesn't match the filter.
    assert list(cql.execute(f'SELECT p, s, c, r FROM {table1} WHERE p={p} AND r=4 ALLOW FILTERING')) == []
