# Copyright 2021 ScyllaDB
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

# Reproducer for issue #7966:
# Test a whole-table scan with filtering which keeps just one row,
# after a long list of non-matching rows. As usual, the scan is done with
# paging, and since most rows do not match the filter, paging should either
# return empty pages or have high latency - but in either case continuing
# the paging should eventually return *all* the matches.
#
# We use two tricks to make reproducing this issue much faster than the
# original reproducer we had for that issue:
# 1. The bug depended on the amount of data being scanned passing some
#    page size limit, so it doesn't matter if the reproducer has a lot of
#    small partitions or fewer long partitions - and inserting fewer long
#    partitions is significantly faster.
# 2. We want the filter to match only at the end the scan - but we don't know
#    know the partition order (we don't want the test to depend on the
#    partitioner) so we scan the table starting at a relatively high token
#    to find a partition near the end of the scan, and use that in the filter.
# Both tricks allowed us to reduce "count" below from 40,000 in the original
# reproducer of #7966 to just 100, and the test's time from 30 seconds to 0.4.
@pytest.mark.xfail(reason="issue #7966")
def test_filtering_with_few_matches(cql, test_keyspace):
    count = 100
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v) VALUES (?, '{long}', ?)")
        for i in range(count):
            cql.execute(stmt, [i, i*100])
        p, v = list(cql.execute(f"SELECT p, v FROM {table} WHERE TOKEN(p) > 8000000000000000000 LIMIT 1"))[0]
        assert list(cql.execute(f"SELECT p FROM {table} WHERE v={v} ALLOW FILTERING")) == [(p,)]

# The following test demonstrates that issue #7966 is specific to a
# partition-range scan, and doesn't happen when scanning a single partition
# scan. We use a much higher count than above to demonstrate that the test still
# passes, but slowly, so we mark this test with "skip" because it's not helpful
# and slow.
@pytest.mark.skip
def test_filtering_single_partition_with_few_matches(cql, test_keyspace):
    count = 1000
    long='x'*60000
    with new_test_table(cql, test_keyspace,
            "p int, c int, s text, v int, PRIMARY KEY (p, c)") as table:
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, v, s) VALUES (1, ?, ?, '{long}')")
        for i in range(count):
            cql.execute(stmt, [i, i])
        assert list(cql.execute(f"SELECT c FROM {table} WHERE p=1 AND v={count-1} ALLOW FILTERING")) == [(count-1,)]
