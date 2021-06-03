# Copyright 2021-present ScyllaDB
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
# Tests for the SELECT requests with various filtering expressions.
# We have a separate test file test_allow_filtering.py, for tests that
# focus on whether the string "ALLOW FILTERING" is needed or not needed
# for a query. In the tests in this file we focus more on the correctness
# of various filtering expressions - regardless of whether ALLOW FILTERING
# is or isn't necessary.

import pytest
from util import new_test_table
from cassandra.protocol import InvalidRequest

# When filtering for "x > 0" or "x < 0", rows with an unset value for x
# should not match the filter.
# Reproduces issue #6295 and its duplicate #8122.
def test_filter_on_unset(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        cql.execute(f"INSERT INTO {table} (a) VALUES (1)")
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (2, 2)")
        cql.execute(f"INSERT INTO {table} (a, b) VALUES (3, -1)")
        assert list(cql.execute(f"SELECT a FROM {table} WHERE b>0 ALLOW FILTERING")) == [(2,)]
        assert list(cql.execute(f"SELECT a FROM {table} WHERE b<0 ALLOW FILTERING")) == [(3,)]
        cql.execute(f"ALTER TABLE {table} ADD c int")
        cql.execute(f"INSERT INTO {table} (a, b,c ) VALUES (4, 5, 6)")
        assert list(cql.execute(f"SELECT a FROM {table} WHERE c<0 ALLOW FILTERING")) == []
        assert list(cql.execute(f"SELECT a FROM {table} WHERE c>0 ALLOW FILTERING")) == [(4,)]

# Reproducers for issue #8203, which test a scan (whole-table or single-
# partition) with filtering which keeps just the last row, after a long list
# of non-matching rows.
# As usual, the scan is done with paging, and since most rows do not match
# the filter, several empty pages should be returned until finally we get
# the expected matching row. If we allow the Python driver to iterate over
# all results, it should read all these pages and give us the one result.
# The bug is that the iteration stops prematurely (it seems after the second
# empty page) and an empty result set is returned.

# Reproducer for issue #8203, partition-range (whole-table) scan case
@pytest.mark.xfail(reason="issue #8203")
def test_filtering_contiguous_nonmatching_partition_range(cql, test_keyspace):
    # The bug depends on the amount of data being scanned passing some
    # page size limit, so it doesn't matter if the reproducer has a lot of
    # small rows or fewer long rows - and inserting fewer long rows is
    # significantly faster.
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v) VALUES (?, '{long}', ?)")
        for i in range(count):
            cql.execute(stmt, [i, i])
        # We want the filter to match only at the end the scan - but we don't
        # know the partition order (we don't want the test to depend on the
        # partitioner). So we first figure out a partition near the end (at
        # some high token), and use that in the filter.
        p, v = list(cql.execute(f"SELECT p, v FROM {table} WHERE TOKEN(p) > 8000000000000000000 LIMIT 1"))[0]
        assert list(cql.execute(f"SELECT p FROM {table} WHERE v={v} ALLOW FILTERING")) == [(p,)]

# Reproducer for issue #8203, single-partition scan case
@pytest.mark.xfail(reason="issue #8203")
def test_filtering_contiguous_nonmatching_single_partition(cql, test_keyspace):
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c int, s text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v, s) VALUES (1, ?, ?, '{long}')")
        for i in range(count):
            cql.execute(stmt, [i, i])
        # To fail this test, we must select s here. If s is not selected,
        # Scylla won't count its size as part of the 1MB limit, and will not
        # return empty pages - the first page will contain the result.
        assert list(cql.execute(f"SELECT c, s FROM {table} WHERE p=1 AND v={count-1} ALLOW FILTERING")) == [(count-1, long)]

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, b int, PRIMARY KEY (a)") as table:
        yield table

# Although the "!=" operator exists in the parser and might be allowed in
# other places (e.g., LWT), it is *NOT* supported in WHERE clauses - not
# for filtering, and also not in relations tha don't need filtering
# (on partition keys or tokens). It is not supported in either Cassandra or
# Scylla, and there are no plans to add this support, so for now the test
# verifies that at least we get the expected error.
def test_operator_ne_not_supported(cql, table1):
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE b != 0 ALLOW FILTERING')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE a != 0')
    with pytest.raises(InvalidRequest, match='Unsupported.*!='):
        cql.execute(f'SELECT a FROM {table1} WHERE token(a) != 0')
