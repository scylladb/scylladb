# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for Light-Weight Transactions (LWT) support in Scylla.
# Note that we have many more LWT tests in the cql-repl framework:
# ../cql/lwt*_test.cql, ../cql/cassandra_cql_test.cql.
#############################################################################

import re
import pytest
from cassandra.protocol import InvalidRequest

from util import new_test_table, unique_key_int

@pytest.fixture(scope="module")
# FIXME: LWT is not supported with tablets yet. See #18066
def table1(cql, test_keyspace_vnodes):
    schema='p int, c int, r int, s int static, PRIMARY KEY(p, c)'
    with new_test_table(cql, test_keyspace_vnodes, schema) as table:
        yield table

# An LWT UPDATE whose condition uses non-static columns begins by reading
# the clustering row which must be specified by the WHERE. If there is a
# static column in the partition, it is read as well. The value of the all
# these columns - regular and static - is then passed to the condition.
# As discovered in issue #10081, if the row determined by WHERE does NOT
# exist, Scylla still needs to read the static column, but forgets to do so.
# this test reproduces this issue.
def test_lwt_missing_row_with_static(cql, table1):
    p = unique_key_int()
    # Insert into partition p just the static column - and no clustering rows.
    cql.execute(f'INSERT INTO {table1}(p, s) values ({p}, 1)')
    # Now, do an update with WHERE p={p} AND c=1. This clustering row does
    # *not* exist, so we expect to see r=null - and s=1 from before.
    r = list(cql.execute(f'UPDATE {table1} SET s=2,r=1 WHERE p={p} AND c=1 IF s=1 and r=null'))
    assert len(r) == 1
    assert r[0].applied == True
    # At this point we should have one row, for c=1 
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, 1, 2, 1)]

# The fact that to reproduce #10081 above we needed the condition (IF) to
# mention a non-static column as well, suggests that Scylla has a different code
# path for the case that the condition has *only* static columns. In fact,
# in that case, the WHERE doesn't even need to specify the clustering key -
# the partition key should be enough. The following test confirms that this
# is indeed the case.
def test_lwt_static_condition(cql, table1):
    p = unique_key_int()
    cql.execute(f'INSERT INTO {table1}(p, s) values ({p}, 1)')
    # When the condition only mentions static (partition-wide) columns,
    # it is allowed not to specify the clustering key in the WHERE:
    r = list(cql.execute(f'UPDATE {table1} SET s=2 WHERE p={p} IF s=1'))
    assert len(r) == 1
    assert r[0].applied == True
    assert list(cql.execute(f'SELECT * FROM {table1} WHERE p={p}')) == [(p, None, 2, None)]
    # When the condition also mentions a non-static column, WHERE must point
    # to a clustering column, i.e., mention the clustering key. If the
    # clustering key is missing, we get an InvalidRequest error, where the
    # message is slightly different between Scylla and Cassandra ("Missing
    # mandatory PRIMARY KEY part c" and "Some clustering keys are missing: c",
    # respectively.
    with pytest.raises(InvalidRequest, match=re.compile('missing', re.IGNORECASE)):
        cql.execute(f'UPDATE {table1} SET s=2 WHERE p={p} IF r=1')

# Generate an LWT update where there is no value for the partition key,
# as the WHERE restricts it using `p = {p} AND p = {p+1}`.
# Such queries are rejected.
def test_lwt_empty_partition_range(cql, table1):
    with pytest.raises(InvalidRequest):
        cql.execute(f"UPDATE {table1} SET r = 9000 WHERE p = 1 AND p = 1000 AND c = 2 IF r = 3")

# Generate an LWT update where there is no value for the clustering key,
# as the WHERE restricts it using `c = 2 AND c = 3`.
# Such queries are rejected.
def test_lwt_empty_clustering_range(cql, table1):
    with pytest.raises(InvalidRequest):
        cql.execute(f"UPDATE {table1} SET r = 9000 WHERE p = 1  AND c = 2 AND c = 2000 IF r = 3")

# In an LWT batch, if one of the condition fails the entire batch is not
# applied. All conditions in a batch use the same values before the batch,
# so if a batch has both a IF EXISTS and IF NOT EXISTS on the same row, they
# can't possibly both be true, so this batch is guaranteed to fail
# regardless of the data. Cassandra detects this specific conflict, and
# prints an error instead of silently failing the batch.
# Reproduces #13011.
@pytest.mark.xfail(reason="issue #13011")
def test_lwt_with_batch_conflict_1(cql, table1):
    p = unique_key_int()
    with pytest.raises(InvalidRequest, match='Cannot mix'):
        cql.execute(f'BEGIN BATCH DELETE FROM {table1} WHERE p={p} AND c=1 IF EXISTS; INSERT INTO {table1}(p,c,r) VALUES ({p},1,2) IF NOT EXISTS; APPLY BATCH;')

# However, Cassandra does not detect every case of a conflict between
# different conditions in a batch. For example, trying both "IF r=1"
# and "IF r=2" returns a not-applied - not an error message.
def test_lwt_with_batch_conflict_2(cql, table1):
    p = unique_key_int()
    rs = list(cql.execute(f'BEGIN BATCH UPDATE {table1} SET r=10 WHERE p={p} AND c=1 IF r=1; UPDATE {table1} SET r=20 WHERE p={p} AND c=1 IF r=2;  APPLY BATCH;'))
    # Note that as a documented difference between Scylla and Cassandra,
    # Cassandra returns just one applied=False in the result r, while
    # Scylla returns a separate row for each of the two conditions.
    for r in rs:
        assert r.applied == False
