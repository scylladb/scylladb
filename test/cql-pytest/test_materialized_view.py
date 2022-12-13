# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for materialized views

import time
import pytest

import nodetool
from util import new_test_table, unique_name, new_materialized_view
from cassandra.protocol import InvalidRequest

# Test that building a view with a large value succeeds. Regression test
# for a bug where values larger than 10MB were rejected during building (#9047)
def test_build_view_with_large_row(cql, test_keyspace):
    schema = 'p int, c int, v text, primary key (p,c)'
    mv = unique_name()
    with new_test_table(cql, test_keyspace, schema) as table:
        big = 'x'*11*1024*1024
        cql.execute(f"INSERT INTO {table}(p,c,v) VALUES (1,1,'{big}')")
        cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT * FROM {table} WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c,p)")
        try:
            retrieved_row = False
            for _ in range(50):
                res = [row for row in cql.execute(f"SELECT * FROM {test_keyspace}.{mv}")]
                if len(res) == 1 and res[0].v == big:
                    retrieved_row = True
                    break
                else:
                    time.sleep(0.1)
            assert retrieved_row
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW {test_keyspace}.{mv}")

# Test that updating a view with a large value succeeds. Regression test
# for a bug where values larger than 10MB were rejected during building (#9047)
def test_update_view_with_large_row(cql, test_keyspace):
    schema = 'p int, c int, v text, primary key (p,c)'
    mv = unique_name()
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT * FROM {table} WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c,p)")
        try:
            big = 'x'*11*1024*1024
            cql.execute(f"INSERT INTO {table}(p,c,v) VALUES (1,1,'{big}')")
            res = [row for row in cql.execute(f"SELECT * FROM {test_keyspace}.{mv}")]
            assert len(res) == 1 and res[0].v == big
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW {test_keyspace}.{mv}")

# Test that a `CREATE MATERIALIZED VIEW` request, that contains bind markers in
# its SELECT statement, fails gracefully with `InvalidRequest` exception and
# doesn't lead to a database crash.
def test_mv_select_stmt_bound_values(cql, test_keyspace):
    schema = 'p int PRIMARY KEY'
    mv = unique_name()
    with new_test_table(cql, test_keyspace, schema) as table:
        try:
            with pytest.raises(InvalidRequest, match="CREATE MATERIALIZED VIEW"):
                cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT * FROM {table} WHERE p = ? PRIMARY KEY (p)")
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW IF EXISTS {test_keyspace}.{mv}")

# In test_null.py::test_empty_string_key() we noticed that an empty string
# is not allowed as a partition key. However, an empty string is a valid
# value for a string column, so if we have a materialized view with this
# string column becoming the view's partition key - the empty string may end
# up being the view row's partition key! This case should be supported,
# because the "IS NOT NULL" clause in the view's declaration does not
# eliminate this row (an empty string is not considered NULL).
# This reproduces issue #9375.
@pytest.mark.xfail(reason="issue #9375")
def test_mv_empty_string_partition_key(cql, test_keyspace):
    schema = 'p int, v text, primary key (p)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
            cql.execute(f"INSERT INTO {table} (p,v) VALUES (123, '')")
            # Note that because cql-pytest runs on a single node, view
            # updates are synchronous, and we can read the view immediately
            # without retrying. In a general setup, this test would require
            # retries.
            # The view row with the empty partition key should exist.
            # In #9375, this failed in Scylla:
            assert list(cql.execute(f"SELECT * FROM {mv}")) == [('', 123)]
            # However, it is still impossible to select just this row,
            # because Cassandra forbids an empty partition key on select
            with pytest.raises(InvalidRequest, match='Key may not be empty'):
                cql.execute(f"SELECT * FROM {mv} WHERE v=''")

# Refs #10851. The code used to create a wildcard selection for all columns,
# which erroneously also includes static columns if such are present in the
# base table. Currently views only operate on regular columns and the filtering
# code assumes that. TODO: once we implement static column support for materialized
# views, this test case will be a nice regression test to ensure that everything still
# works if the static columns are *not* used in the view.
def test_filter_with_unused_static_column(cql, test_keyspace):
    schema = 'p int, c int, v int, s int static, primary key (p,c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, select='p,c,v', pk='p,c,v', where='p IS NOT NULL and c IS NOT NULL and v = 44') as mv:
            cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (42,43,44)")
            cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (1,2,3)")
            assert list(cql.execute(f"SELECT * FROM {mv}")) == [(42, 43, 44)]

# Reproducer for issue #9450 - when a view's key column name is a (quoted)
# keyword, writes used to fail because they generated internally broken CQL
# with the column name not quoted.
def test_mv_quoted_column_names(cql, test_keyspace):
    for colname in ['"dog"', '"Dog"', 'DOG', '"to"', 'int']:
        with new_test_table(cql, test_keyspace, f'p int primary key, {colname} int') as table:
            with new_materialized_view(cql, table, '*', f'{colname}, p', f'{colname} is not null and p is not null') as mv:
                cql.execute(f'INSERT INTO {table} (p, {colname}) values (1, 2)')
                # Validate that not only the write didn't fail, it actually
                # write the right thing to the view. NOTE: on a single-node
                # Scylla, view update is synchronous so we can just read and
                # don't need to wait or retry.
                assert list(cql.execute(f'SELECT * from {mv}')) == [(2, 1)]

# Same as test_mv_quoted_column_names above (reproducing issue #9450), just
# check *view building* - i.e., pre-existing data in the base table that
# needs to be copied to the view. The view building cannot return an error
# to the user, but can fail to write the desired data into the view.
def test_mv_quoted_column_names_build(cql, test_keyspace):
    for colname in ['"dog"', '"Dog"', 'DOG', '"to"', 'int']:
        with new_test_table(cql, test_keyspace, f'p int primary key, {colname} int') as table:
            cql.execute(f'INSERT INTO {table} (p, {colname}) values (1, 2)')
            with new_materialized_view(cql, table, '*', f'{colname}, p', f'{colname} is not null and p is not null') as mv:
                # When Scylla's view builder fails as it did in issue #9450,
                # there is no way to tell this state apart from a view build
                # that simply hasn't completed (besides looking at the logs,
                # which we don't). This means, unfortunately, that a failure
                # of this test is slow - it needs to wait for a timeout.
                start_time = time.time()
                while time.time() < start_time + 30:
                    if list(cql.execute(f'SELECT * from {mv}')) == [(2, 1)]:
                        break
                assert list(cql.execute(f'SELECT * from {mv}')) == [(2, 1)]

# Reproduces #11668
# When the view builder resumes building a partition, it reuses the reader
# used from the previous step but re-creates the compactor. This means that any
# range tombstone changes active at the time of suspending the step, have to be
# explicitly re-opened on when resuming. Without that, already deleted base rows
# can be resurrected as demonstrated by this test.
# The view-builder suspends processing a base-table after
# `view_builder::batch_size` (that is 128) rows. So in this test we create a
# table which has at least 2X that many rows and add a range tombstone so that
# it covers half of the rows (even rows are covered why odd rows aren't).
def test_view_builder_suspend_with_active_range_tombstone(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY(pk, ck)", "WITH compaction = {'class': 'NullCompactionStrategy'}") as table:
        stmt = cql.prepare(f'INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)')

        # sstable 1 - even rows
        for ck in range(0, 512, 2):
            cql.execute(stmt, (0, ck, ck))
        nodetool.flush(cql, table)

        # sstable 2 - odd rows and a range tombstone covering even rows
        # we need two sstables so memtable doesn't compact away the shadowed rows
        cql.execute(f"DELETE FROM {table} WHERE pk = 0 AND ck >= 0 AND ck < 512")
        for ck in range(1, 512, 2):
            cql.execute(stmt, (0, ck, ck))
        nodetool.flush(cql, table)

        # we should not see any even rows here - they are covered by the range tombstone
        res = [r.ck for r in cql.execute(f"SELECT ck FROM {table} WHERE pk = 0")]
        assert res == list(range(1, 512, 2))

        with new_materialized_view(cql, table, select='*', pk='v,pk,ck', where='v is not null and pk is not null and ck is not null') as mv:
            start_time = time.time()
            while time.time() < start_time + 30:
                res = sorted([r.v for r in cql.execute(f"SELECT * FROM {mv}")])
                if len(res) >= 512/2:
                    break
                time.sleep(0.1)
            # again, we should not see any even rows in the materialized-view,
            # they are covered with a range tombstone in the base-table
            assert res == list(range(1, 512, 2))

# A variant of the above using a partition-tombstone, which is also lost similar
# to range tombstones.
def test_view_builder_suspend_with_partition_tombstone(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY(pk, ck)", "WITH compaction = {'class': 'NullCompactionStrategy'}") as table:
        stmt = cql.prepare(f'INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)')

        # sstable 1 - even rows
        for ck in range(0, 512, 2):
            cql.execute(stmt, (0, ck, ck))
        nodetool.flush(cql, table)

        # sstable 2 - odd rows and a partition covering even rows
        # we need two sstables so memtable doesn't compact away the shadowed rows
        cql.execute(f"DELETE FROM {table} WHERE pk = 0")
        for ck in range(1, 512, 2):
            cql.execute(stmt, (0, ck, ck))
        nodetool.flush(cql, table)

        # we should not see any even rows here - they are covered by the partition tombstone
        res = [r.ck for r in cql.execute(f"SELECT ck FROM {table} WHERE pk = 0")]
        assert res == list(range(1, 512, 2))

        with new_materialized_view(cql, table, select='*', pk='v,pk,ck', where='v is not null and pk is not null and ck is not null') as mv:
            start_time = time.time()
            while time.time() < start_time + 30:
                res = sorted([r.v for r in cql.execute(f"SELECT * FROM {mv}")])
                if len(res) >= 512/2:
                    break
                time.sleep(0.1)
            # again, we should not see any even rows in the materialized-view,
            # they are covered with a partition tombstone in the base-table
            assert res == list(range(1, 512, 2))

# Reproducer for issue #11542 and #10026: We have a table with with a
# materialized view with a filter and some data, at which point we modify
# the base table (e.g., add some silly comment) and then try to modify the
# data. The last modification used to fail, logging "Column definition v
# does not match any column in the query selection".
# The same test without the silly base-table modification works, and so does
# the same test without the filter in the materialized view that uses the
# base-regular column v. So does the same test without pre-modification data.
#
# This test is Scylla-only because Cassandra does not support filtering
# on a base-regular column v that is only a key column in the view.
def test_view_update_and_alter_base(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int primary key, v int') as table:
        with new_materialized_view(cql, table, '*', 'v, p', 'v >= 0 and p is not null') as mv:
            cql.execute(f'INSERT INTO {table} (p,v) VALUES (1,1)')
            # In our tests, MV writes are synchronous, so we can read
            # immediately
            assert len(list(cql.execute(f"SELECT v from {mv}"))) == 1
            # Alter the base table, with a silly comment change that doesn't
            # change anything important - but still the base schema changes.
            cql.execute(f"ALTER TABLE {table} WITH COMMENT = '{unique_name()}'")
            # Try to modify an item. This failed in #11542.
            cql.execute(f'UPDATE {table} SET v=-1 WHERE p=1')
            assert len(list(cql.execute(f"SELECT v from {mv}"))) == 0

# Reproducer for issue #12297, reproducing a specific way in which a view
# table could be made inconsistent with the base table:
# The test writes 500 rows to one partition in a base table, and then uses
# USING TIMESTAMP with the right value to cause a base partition deletion
# which deletes not the entire partition but just its last 50 rows. As the
# 50 rows of the base partition get deleted, we expect 50 rows from the
# view table to also get deleted - but bug #12297 was that this wasn't
# happening - rather, all rows remained in the view.
# The bug cannot be reproduced with 100 rows (and deleting the last 10)
# but 113 rows (and 101 rows after deleting the last 12) does reproduce
# it. Reproducing the bug also required a setup where USING TIMESTAMP
# deleted the *last* rows - using it to delete the *first* rows did not
# have a bug (the view rows were deleted fine).
@pytest.mark.parametrize("size", [100, 113, 500])
def test_long_skipped_view_update_delete_with_timestamp(cql, test_keyspace, size):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)') as table:
        with new_materialized_view(cql, table, '*', 'p, x, c', 'p is not null and x is not null and c is not null') as mv:
            # Write size rows with c=0..(size-1). Because the iteration is in
            # reverse order, the first row in clustering order (c=0) will
            # have the latest write timestamp.
            for i in reversed(range(size)):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 1")) == list(cql.execute(f"SELECT c FROM {mv} WHERE p = 1"))
            # Get the timestamp of the size*0.9th item. Because we wrote items
            # in reverse, items 0.9-1.0*size all have earlier timestamp than
            # that.
            t = list(cql.execute(f"SELECT writetime(y) FROM {table} WHERE p = 1 and c = {int(size*0.9)}"))[0].writetime_y
            cql.execute(f'DELETE FROM {table} USING TIMESTAMP {t} WHERE p=1')
            # After the deletion we expect to see size*0.9 rows remaining
            # (timestamp ties cannot happen for separate writes, if they
            # did we could have a bit less), but most importantly, the view
            # should have exactly the same rows.
            assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 1")) == list(cql.execute(f"SELECT c FROM {mv} WHERE p = 1"))

# Same test as above, just that in this version the view partition key is
# different from the base's, so we can be sure that Scylla needs to go
# through the loop of deleting many view rows and cannot delete an entire
# view partition in one fell swoop. In the above test, Scylla *may* contain
# such an optimization (currently it doesn't), so it may reach a different
# code path.
def test_long_skipped_view_update_delete_with_timestamp2(cql, test_keyspace):
    size = 200
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)') as table:
        with new_materialized_view(cql, table, '*', 'x, p, c', 'p is not null and x is not null and c is not null') as mv:
            for i in reversed(range(size)):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            assert list(cql.execute(f"SELECT c FROM {table}")) == sorted(list(cql.execute(f"SELECT c FROM {mv}")))
            t = list(cql.execute(f"SELECT writetime(y) FROM {table} WHERE p = 1 and c = {int(size*0.9)}"))[0].writetime_y
            cql.execute(f'DELETE FROM {table} USING TIMESTAMP {t} WHERE p=1')
            assert list(cql.execute(f"SELECT c FROM {table}")) == sorted(list(cql.execute(f"SELECT c FROM {mv}")))

# Another, more fundemental, reproducer for issue #12297 where a certain
# modification to a base partition modifing more than 100 rows was not
# applied to the view beyond the 100th row.
# The test above, test_long_skipped_view_update_delete_with_timestamp was one
# such specific case, which involved a partition tombstone and a specific
# choice of timestamp which causes the first 100 rows to NOT be changed.
# In this test we show that the bug is not just about do-nothing tombstones:
# In any base modification which involves more than 100 rows, if the first
# 100 rows don't change the view (as decided by the can_skip_view_updates()
# function), the other rows are wrongly skipped at well and not applied to
# the view!
# The specific case we use here is an update that sets some irrelevant
# (not-selected-by-the-view) column y on 200 rows, and additionally writes
# a new row as the 201st row. With bug #12297, that 201st row will be
# missing in the view.
def test_long_skipped_view_update_irrelevant_column(cql, test_keyspace):
    size = 200
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)') as table:
        # Note that column "y" is not selected by the materialized view
        with new_materialized_view(cql, table, 'p, x, c', 'p, x, c', 'p is not null and x is not null and c is not null') as mv:
            for i in range(size):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            # In a single batch (a single mutation), update "y" column in all
            # 'size' existing rows, plus add one new row in the last position
            # (the partition is sorted by the "c" column). The first 'size'
            # UPDATEs can be skipped in the view (because y isn't selected),
            # but the last INSERT can't be skipped - it really adds a new row.
            cmd = 'BEGIN BATCH '
            for i in range(size):
                cmd += f'UPDATE {table} SET y=7 where p=1 and c={i}; '
            cmd += f'INSERT INTO {table} (p,c,x,y) VALUES (1,{size+1},{size+1},{size+1}); '
            cmd += 'APPLY BATCH;'
            cql.execute(cmd)
            # We should now have the same size+1 rows in both base and view
            assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 1")) == list(cql.execute(f"SELECT c FROM {mv} WHERE p = 1"))

# After the previous tests checked elaborate conditions where modifying a
# base-table partition resulted in many skipped view updates, let's also
# check the more basic situation where the base-table partition modification
# (in this case, a deletion) result in many view-table updates, and all
# of them should happen even if the code needs to do it internally in
# several batches of 100 (for example).
def test_mv_long_delete(cql, test_keyspace):
    size = 300
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)') as table:
        with new_materialized_view(cql, table, '*', 'p, x, c', 'p is not null and x is not null and c is not null') as mv:
            for i in range(size):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            cql.execute(f'DELETE FROM {table} WHERE p=1')
            assert list(cql.execute(f"SELECT c FROM {table} WHERE p = 1")) == []
            assert list(cql.execute(f"SELECT c FROM {mv} WHERE p = 1")) == []
