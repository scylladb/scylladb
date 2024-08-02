# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for materialized views

import time
import pytest

from util import new_test_table, unique_name, new_materialized_view, ScyllaMetrics
from cassandra.protocol import InvalidRequest, SyntaxException

import nodetool

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
# up being the view row's partition key. This case should be supported,
# because the "IS NOT NULL" clause in the view's declaration does not
# eliminate this row (an empty string is *not* considered NULL).
# Reproduces issue #9375.
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
            # Verify that we can flush an sstable with just an one partition
            # with an empty-string key (in the past we had a summary-file
            # sanity check preventing this from working).
            nodetool.flush(cql, mv)

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

# The previous test (test_mv_empty_string_partition_key) verifies that a
# row with an empty-string partition key can appear in the view. This was
# checked with a full-table scan. This test is about reading this one
# view partition individually, with WHERE v=''.
# Surprisingly, Cassandra does NOT allow to SELECT this specific row
# individually - "WHERE v=''" is not allowed when v is the partition key
# (even of a view). We consider this to be a Cassandra bug - it doesn't
# make sense to allow the user to add a row and to see it in a full-table
# scan, but not to query it individually. This is why we mark this test as
# a Cassandra bug and want Scylla to pass it.
# Reproduces issue #9375 and #9352.
def test_mv_empty_string_partition_key_individual(cassandra_bug, cql, test_keyspace):
    schema = 'p int, v text, primary key (p)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
            # Insert a bunch of (p,v) rows. One of the v's is the empty
            # string, which we would like to test, but let's insert more
            # rows to make it more likely to exercise various possibilities
            # of token ordering (see #9352).
            rows = [[123, ''], [1, 'dog'], [2, 'cat'], [700, 'hello'], [3, 'horse']]
            for row in rows:
                cql.execute(f"INSERT INTO {table} (p,v) VALUES ({row[0]}, '{row[1]}')")
            # Note that because cql-pytest runs on a single node, view
            # updates are synchronous, and we can read the view immediately
            # without retrying. In a general setup, this test would require
            # retries.
            # Check that we can read the individual partition with the
            # empty-string key:
            assert list(cql.execute(f"SELECT * FROM {mv} WHERE v=''")) == [('', 123)]
            # The SELECT above works from cache. However, empty partition
            # keys also used to be special-cased and be buggy when reading
            # and writing sstables, so let's verify that the empty partition
            # key can actually be written and read from disk, by forcing a
            # memtable flush and bypassing the cache on read.
            # In the past Scylla used to fail this flush because the sstable
            # layer refused to write empty partition keys to the sstable:
            nodetool.flush(cql, mv)
            # First try a full-table scan, and then try to read the
            # individual partition with the empty key:
            assert set(cql.execute(f"SELECT * FROM {mv} BYPASS CACHE")) == {
                (x[1], x[0]) for x in rows}
            # Issue #9352 used to prevent us finding WHERE v='' here, even
            # when the data is known to exist (the above full-table scan
            # saw it!) and despite the fact that WHERE v='' is parsed
            # correctly because we tested above it works from memtables.
            assert list(cql.execute(f"SELECT * FROM {mv} WHERE v='' BYPASS CACHE")) == [('', 123)]

# Test that the "IS NOT NULL" clause in the materialized view's SELECT
# functions as expected - namely, rows which have their would-be view
# key column unset (aka null) do not get copied into the view.
def test_mv_is_not_null(cql, test_keyspace):
    schema = 'p int, v text, primary key (p)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
            cql.execute(f"INSERT INTO {table} (p,v) VALUES (123, 'dog')")
            cql.execute(f"INSERT INTO {table} (p,v) VALUES (17, null)")
            # Note that because cql-pytest runs on a single node, view
            # updates are synchronous, and we can read the view immediately
            # without retrying. In a general setup, this test would require
            # retries.
            # The row with 123 should appear in the view, but the row with
            # 17 should not, because v *is* null.
            assert list(cql.execute(f"SELECT * FROM {mv}")) == [('dog', 123)]
            # The view row should disappear and reappear if its key is
            # changed to null and back in the base table:
            cql.execute(f"UPDATE {table} SET v=null WHERE p=123")
            assert list(cql.execute(f"SELECT * FROM {mv}")) == []
            cql.execute(f"UPDATE {table} SET v='cat' WHERE p=123")
            assert list(cql.execute(f"SELECT * FROM {mv}")) == [('cat', 123)]
            cql.execute(f"DELETE v FROM {table} WHERE p=123")
            assert list(cql.execute(f"SELECT * FROM {mv}")) == []

# Refs #10851. The code used to create a wildcard selection for all columns,
# which erroneously also includes static columns if such are present in the
# base table. Currently views only operate on regular columns and the filtering
# code assumes that. Once we implement static column support for materialized
# views, this test case will be a nice regression test to ensure that everything still
# works if the static columns are *not* used in the view.
# This test goes over all combinations of filters for partition, clustering and regular
# base columns.
def test_filter_with_unused_static_column(cql, test_keyspace, scylla_only):
    schema = 'p int, c int, v int, s int static, primary key (p,c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        for p_condition in ['p = 42', 'p IS NOT NULL']:
            for c_condition in ['c = 43', 'c IS NOT NULL']:
                for v_condition in ['v = 44', 'v IS NOT NULL']:
                    where = f"{p_condition} AND {c_condition} AND {v_condition}"
                    with new_materialized_view(cql, table, select='p,c,v', pk='p,c,v', where=where) as mv:
                        cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (42,43,44)")
                        cql.execute(f"INSERT INTO {table} (p,c,v) VALUES (1,2,3)")
                        expected = [(42,43,44)] if '4' in where else [(42,43,44),(1,2,3)]
                        assert list(cql.execute(f"SELECT * FROM {mv}")) == expected

# Ensure that we don't allow materialized views which contain static rows.
# Neither Cassandra nor Scylla support this at the moment.
def test_static_columns_are_disallowed(cql, test_keyspace):
    schema = 'p int, c int, v int, s int static, primary key (p,c)'
    with new_test_table(cql, test_keyspace, schema) as table:
        # Case 1: 's' not in primary key
        mv = unique_name()
        try:
            with pytest.raises(InvalidRequest, match="[Ss]tatic column"):
                cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT p, s FROM {table} PRIMARY KEY (p)")
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW IF EXISTS {test_keyspace}.{mv}")

        # Case 2: 's' in primary key
        mv = unique_name()
        try:
            with pytest.raises(InvalidRequest, match="[Ss]tatic column"):
                cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT p, s FROM {table} WHERE s IS NOT NULL PRIMARY KEY (s, p)")
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW IF EXISTS {test_keyspace}.{mv}")

# IS_NOT operator can only be used in the context of materialized view creation and it must be of the form IS NOT NULL.
# Trying to do something like IS NOT 42 should fail.
# The error is a SyntaxException because Scylla and Cassandra check this during parsing.
def test_is_not_operator_must_be_null(cql, test_keyspace):
    schema = 'p int PRIMARY KEY'
    mv = unique_name()
    with new_test_table(cql, test_keyspace, schema) as table:
        try:
            with pytest.raises(SyntaxException, match="NULL"):
                cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT * FROM {table} WHERE p IS NOT 42 PRIMARY KEY (p)")
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW IF EXISTS {test_keyspace}.{mv}")

# The IS NOT NULL operator was first added to Cassandra and Scylla for use
# just in key columns in materialized views. It was not supported in general
# filters in SELECT (see issue #8517), and in particular cannot be used in
# a materialized-view definition as a filter on non-key columns. However,
# if this usage is not allowed, we expect to see a clear error and not silently
# ignoring the IS NOT NULL condition as happens in issue #10365.
#
# NOTE: if issue #8517 (IS NOT NULL in filters) is implemented, we will need to
# replace this test by a test that checks that the filter works as expected,
# both in ordinary base-table SELECT and in materialized-view definition.
def test_is_not_null_forbidden_in_filter(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int primary key, xyz int') as table:
        # Check that "IS NOT NULL" is not supported in a regular (base table)
        # SELECT filter. Cassandra reports an InvalidRequest: "Unsupported
        # restriction: xyz IS NOT NULL". In Scylla the message is different:
        # "restriction '(xyz) IS NOT { null }' is only supported in materialized
        # view creation".
        #
        with pytest.raises(InvalidRequest, match="xyz"):
            cql.execute(f'SELECT * FROM {table} WHERE xyz IS NOT NULL ALLOW FILTERING')
        # Check that "xyz IS NOT NULL" is also not supported in a
        # materialized-view definition (where xyz is not a key column)
        # Reproduces #8517
        mv = unique_name()
        try:
            with pytest.raises(InvalidRequest, match="xyz"):
                cql.execute(f"CREATE MATERIALIZED VIEW {test_keyspace}.{mv} AS SELECT * FROM {table} WHERE p IS NOT NULL AND xyz IS NOT NULL PRIMARY KEY (p)")
                # There is no need to continue the test - if the CREATE
                # MATERIALIZED VIEW above succeeded, it is already not what we
                # expect without #8517. However, let's demonstrate that it's
                # even worse - not only does the "xyz IS NOT NULL" not generate
                # an error, it is outright ignored and not used in the filter.
                # If it weren't ignored, it should filter out partition 124
                # in the following example:
                cql.execute(f"INSERT INTO {table} (p,xyz) VALUES (123, 456)")
                cql.execute(f"INSERT INTO {table} (p) VALUES (124)")
                assert sorted(list(cql.execute(f"SELECT p FROM {test_keyspace}.{mv}")))==[(123,)]
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW IF EXISTS {test_keyspace}.{mv}")

# Test that a view can be altered with synchronous_updates property and that
# the synchronous updates code path is then reached for such view.
# The synchronous_updates feature is a ScyllaDB extension, so this is a
# scylla_only test.
def test_mv_synchronous_updates(cql, test_keyspace, scylla_only):
    schema = 'p int, v text, primary key (p)'
    with new_test_table(cql, test_keyspace, schema) as table:
        with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as sync_mv, \
             new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as async_mv, \
             new_materialized_view(cql, table, '*', 'v,p', 'v is not null and p is not null', extra='with synchronous_updates = true') as sync_mv_from_the_start, \
             new_materialized_view(cql, table, '*', 'v,p', 'v is not null and p is not null', extra='with synchronous_updates = true') as async_mv_altered:
            # Make one view synchronous
            cql.execute(f"ALTER MATERIALIZED VIEW {sync_mv} WITH synchronous_updates = true")
            # Make another one asynchronous
            cql.execute(f"ALTER MATERIALIZED VIEW {async_mv_altered} WITH synchronous_updates = false")

            # Execute a query and inspect its tracing info
            res = cql.execute(f"INSERT INTO {table} (p,v) VALUES (123, 'dog')", trace=True)
            trace = res.get_query_trace()

            wanted_trace1 = f"Forcing {sync_mv} view update to be synchronous"
            wanted_trace2 = f"Forcing {sync_mv_from_the_start} view update to be synchronous"
            unwanted_trace1 = f"Forcing {async_mv} view update to be synchronous"
            unwanted_trace2 = f"Forcing {async_mv_altered} view update to be synchronous"

            wanted_traces_were_found = [False, False]
            for event in trace.events:
                assert unwanted_trace1 not in event.description
                assert unwanted_trace2 not in event.description
                if wanted_trace1 in event.description:
                    wanted_traces_were_found[0] = True
                if wanted_trace2 in event.description:
                    wanted_traces_were_found[1] = True
            assert all(wanted_traces_were_found)

# Reproduces #8627:
# Whereas regular columns values are limited in size to 2GB, key columns are
# limited to 64KB. This means that if a certain column is regular in the base
# table but a key in one of its views, we cannot write to this regular column
# an over-64KB value. Ideally, such a write should fail cleanly with an
# InvalidQuery.
# But today, neither Cassandra nor Scylla does this correctly. Both do not
# detect the problem at the coordinator level, and both send the writes to the
# replicas and fail the view update in each replica. The user's write may or
# may not fail depending on whether the view update is done synchronously
# (Scylla, sometimes) or asynchrhonously (Casandra). But even in the failure
# case the failure does not explain why the replica writes failed - the only
# message about a key being too long appears in the log.
# Note that the same issue also applies to secondary indexes, and this is
# tested in test_secondary_index.py.
@pytest.mark.xfail(reason="issue #8627")
def test_oversized_base_regular_view_key(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int primary key, v text') as table:
        with new_materialized_view(cql, table, select='*', pk='v,p', where='v is not null and p is not null') as mv:
            big = 'x'*66536
            with pytest.raises(InvalidRequest, match='size'):
                cql.execute(f"INSERT INTO {table}(p,v) VALUES (1,'{big}')")
            # Ideally, the entire write operation should be considered
            # invalid, and no part of it will be done. In particular, the
            # base write will also not happen.
            assert [] == list(cql.execute(f"SELECT * FROM {table} WHERE p=1"))

# Reproduces #8627:
# Same as test_oversized_base_regular_view_key above, just check *view
# building*- i.e., pre-existing data in the base table that needs to be
# copied to the view. The view building cannot return an error to the user,
# but we do expect it to skip the problematic row and continue to complete
# the rest of the view build.
@pytest.mark.xfail(reason="issue #8627")
# This test currently breaks the build (it repeats a failing build step,
# and never complete) and we cannot quickly recognize this failure, so
# to avoid a very slow failure, we currently "skip" this test.
@pytest.mark.skip(reason="issue #8627, fails very slow")
def test_oversized_base_regular_view_key_build(cql, test_keyspace, cassandra_bug):
    with new_test_table(cql, test_keyspace, 'p int primary key, v text') as table:
        # No materialized view yet - a "big" value in v is perfectly fine:
        stmt = cql.prepare(f'INSERT INTO {table} (p,v) VALUES (?, ?)')
        for i in range(30):
            cql.execute(stmt, [i, str(i)])
        big = 'x'*66536
        cql.execute(stmt, [30, big])
        assert [(30,big)] == list(cql.execute(f'SELECT * FROM {table} WHERE p=30'))
        # Add a materialized view with v as the new key. The view build,
        # copying data from the base table to the view, should start promptly.
        with new_materialized_view(cql, table, select='*', pk='v,p', where='v is not null and p is not null') as mv:
            # If Scylla's view builder hangs or stops, there is no way to
            # tell this state apart from a view build that simply hasn't
            # completed yet (besides looking at the logs, which we don't).
            # This means, unfortunately, that a failure of this test is slow -
            # it needs to wait for a timeout.
            start_time = time.time()
            while time.time() < start_time + 30:
                results = set(list(cql.execute(f'SELECT * from {mv}')))
                # The oversized "big" cannot be a key in the view, so
                # shouldn't be in results:
                assert not (big, 30) in results
                print(results)
                # The rest of the items in the base table should be in
                # the view:
                if results == {(str(i), i) for i in range(30)}:
                        break
                time.sleep(0.1)
            assert results == {(str(i), i) for i in range(30)}

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

# Test when IS NOT NULL is required, vs. not required, for the key columns
# of a materialized view WHERE clause.
# In general, the user needs to add a IS NOT NULL for each and every key
# column of the view in the view's WHERE clause, to emphasize that when
# a row has a null value for that column - the row will be missing from
# the view (because null key columns are not allowed).
# However, one can argue that if one of the view's key columns was already
# a base key column, then it is already known that this column cannot ever
# be null, so it is pointless to require the "IS NOT NULL". However,
# Cassandra still requires "IS NOT NULL" on any column - even base key
# columns.
# This test reproduces issue issue #11979, that Scylla used to require
# IS NOT NULL inconsistently.
@pytest.mark.xfail(reason="issue #11979")
def test_is_not_null_requirement(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p, c)') as table:
        # missing "v is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p,c,v', where='p is not null and c is not null') as mv:
                pass
        # missing "c is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p,c,v', where='v is not null and p is not null') as mv:
                pass
        # missing "p is not null":
        # This check reproduces issue #11979:
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p,c,v', where='c is not null and v is not null') as mv:
                pass
    # Similar test, with composite keys
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, c1 int, c2 int, v int, primary key ((p1, p2), c1, c2)') as table:
        # missing "p1 is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p1,p2,c1,c2,v', where='p2 is not null and c1 is not null and c2 is not null and v is not null') as mv:
                pass
        # missing "p2 is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p1,p2,c1,c2,v', where='p1 is not null and c1 is not null and c2 is not null and v is not null') as mv:
                pass
        # missing "c1 is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p1,p2,c1,c2,v', where='p1 is not null and p2 is not null and c2 is not null and v is not null') as mv:
                pass
        # missing "c2 is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p1,p2,c1,c2,v', where='p1 is not null and p2 is not null and c1 is not null and v is not null') as mv:
                pass
        # missing "v is not null":
        with pytest.raises(InvalidRequest, match="IS NOT NULL"):
            with new_materialized_view(cql, table, select='*', pk='p1,p2,c1,c2,v', where='p1 is not null and p2 is not null and c1 is not null and c2 is not null') as mv:
                pass

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

# Another, more fundamental, reproducer for issue #12297 where a certain
# modification to a base partition modifying more than 100 rows was not
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

# Several tests for how "CLUSTERING ORDER BY" interacts with materialized
# views:

# In Cassandra, when a base table has a reversed-order clustering column and
# this column is used in a materialized view, its order in the view inherits
# the same reversed sort order it had in the base table.
# Reproduces #12308
@pytest.mark.xfail(reason="issue #12308")
def test_mv_inherit_clustering_order(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)', 'with clustering order by (c DESC)') as table:
        # note no explicit clustering order on c in the materialized view:
        with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null') as mv:
            for i in range(4):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            # The base table's clustering order is reversed, and it should
            # also be in the view (at least, it's so in Cassandra).
            assert list(cql.execute(f'SELECT y from {table}')) == [(3,),(2,),(1,),(0,)]
            assert list(cql.execute(f'SELECT y from {mv}')) == [(3,),(2,),(1,),(0,)]

# When a materialized view specification declares the clustering keys of
# they view, they default to the base table's clustering order (see test
# above), but the order can be overridden by an explicit "with clustering
# order by" in the materialized view definition:
def test_mv_override_clustering_order_1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)', 'with clustering order by (c DESC)') as table:
        # explicitly reverse the clustering order of "c" to be ascending.
        # note that if we specify c's clustering order, we are also forced
        # to specify x's even though we just want it to be the default:
        with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c ASC, x ASC)') as mv:
            for i in range(4):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            # The base table's clustering order is descending, but in the view
            # it should be ascending.
            assert list(cql.execute(f'SELECT y from {table}')) == [(3,),(2,),(1,),(0,)]
            assert list(cql.execute(f'SELECT y from {mv}')) == [(0,),(1,),(2,),(3,)]

def test_mv_override_clustering_order_2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)', 'with clustering order by (c ASC)') as table:
        # explicitly reverse the clustering order of "c" to be descending.
        # note that if we specify c's clustering order, we are also forced
        # to specify x's even though we just want it to be the default:
        with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c DESC, x ASC)') as mv:
            for i in range(4):
                cql.execute(f'INSERT INTO {table} (p,c,x,y) VALUES (1,{i},{i},{i})')
            # The base table's clustering order is ascending, but in the view
            # it should be descending.
            assert list(cql.execute(f'SELECT y from {table}')) == [(0,),(1,),(2,),(3,)]
            assert list(cql.execute(f'SELECT y from {mv}')) == [(3,),(2,),(1,),(0,)]

# Another test for CLUSTERING ORDER BY, using quoted and unquoted column
# names and checking they are matched properly
def test_mv_override_clustering_order_quoted(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, "Hello" int, primary key (p,c)') as table:
        # X and "x" are the same as x:
        with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c DESC, X ASC)') as mv:
            pass
        with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c DESC, "x" ASC)') as mv:
            pass
        # But "Hello" is not the same as "HELLO" or hello
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, "Hello"', 'p is not null and c is not null and "Hello" is not null', 'with clustering order by (c DESC, hello ASC)') as mv:
                pass
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, "Hello"', 'p is not null and c is not null and "Hello" is not null', 'with clustering order by (c DESC, "HELLO" ASC)') as mv:
                pass

# Cassandra requires that if we specify WITH CLUSTERING ORDER BY in the
# materialized view definition, it must mention all clustering key columns
# defined in the view's PRIMARY KEY, in that same order. If the columns are
# mis-ordered or one is missing, the statement is rejected with the message
# "Clustering key columns must exactly match columns in CLUSTERING ORDER BY
# directive". The reason for this rejection is that CLUSTERING ORDER BY
# with a misordered or partial list of clustering columns may wrongly suggest
# that this list determines the order of clustering columns when comparing
# them - when in fact the PRIMARY KEY specification controls that order.
# The following test verifies that these bad WITH CLUSTERING ORDER BY
# clauses are indeed rejected.
# Reproduces #12936.
@pytest.mark.xfail(reason="issue #12936")
def test_mv_override_clustering_order_bad1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)') as table:
        # Mis-ordered clustering columns: c,x on PRIMARY KEY, but
        # x,c in WITH CLUSTERING ORDER:
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'WITH CLUSTERING ORDER BY (x ASC, c ASC)') as mv:
                pass
        # Missing clustering columns: c,x on PRIMARY KEY, but
        # x or c in WITH CLUSTERING ORDER:
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'WITH CLUSTERING ORDER BY (c ASC)') as mv:
                pass
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'WITH CLUSTERING ORDER BY (x ASC)') as mv:
                pass
        # Duplicate clustering column: c,x on PRIMARY KEY, but c,x,x
        # (with same or different order for x) in WITH CLUSTERING ORDER:
        for order in ['c ASC, x ASC, x ASC',
                      'c ASC, x ASC, x DESC',
                      'c ASC, c ASC, x ASC',
                      'c ASC, c DESC, x ASC']:
            with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
                with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', f'WITH CLUSTERING ORDER BY ({order})') as mv:
                    pass

# Cassandra is strict about the WITH CLUSTERING ORDER BY clause in the
# definition of the materialized view that must, if it exists, list all
# the view's clustering keys. Scylla was less strict (the above test
# test_mv_override_clustering_order_bad failed), but in any case we should
# not allow to list spurious names of non-clustering keys in the CLUSTERING
# ORDER BY clause. Reproduces #10767.
def test_mv_override_clustering_order_bad2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, x int, y int, primary key (p,c)') as table:
        # Only a non-clustering-key column y (clustering key c and x missing):
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (y DESC)') as mv:
                pass
        # The two clustering key column (c and x) plus a regular column y
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c ASC, x ASC, y DESC)') as mv:
                pass
        # The two clustering key column (c and x) plus a partition key p
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c ASC, x ASC, p DESC)') as mv:
                pass
        # The two clustering key column (c and x) plus non-existent z
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by (c ASC, x ASC, z DESC)') as mv:
                pass
        # The clustering key column in the base (c) but it's no longer
        # a clustering key column in the view so can't be ordered
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'c, p', 'p is not null and c is not null', 'with clustering order by (c ASC)') as mv:
                pass
        # Check that the case of quoted names is supported correctly,
        # "X" and x are not the same
        with pytest.raises(InvalidRequest, match="CLUSTERING ORDER BY"):
            with new_materialized_view(cql, table, '*', 'p, c, x', 'p is not null and c is not null and x is not null', 'with clustering order by ("X" ASC)') as mv:
                pass

# Test views that only refer to the primary key, exercising the invisible
# empty type columns that are injected into the view schema in order to
# compute the view row liveness.
#
# scylla_only because Cassandra doesn't support synchronous updates.
def test_mv_with_only_primary_key_rows(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id int PRIMARY KEY, v1 int, v2 int') as base:
        # Use a synchronous view so we don't have to worry about races between flush and
        # view updates.
        with new_materialized_view(cql, table=base, select='id', pk='id', where='id IS NOT NULL',
                extra='WITH synchronous_updates = true') as view:
            cql.execute(f'INSERT INTO {base} (id, v1) VALUES (1, 0)')
            cql.execute(f'INSERT INTO {base} (id, v2) VALUES (2, 0)')
            cql.execute(f'INSERT INTO {base} (id) VALUES (3)')
            # The following row is kept alive by the liveness of v1, since it doesn't have a row marker
            cql.execute(f'UPDATE {base} SET v1 = 7 WHERE id = 4')
            nodetool.flush(cql, view)
            assert(set([row.id for row in cql.execute(f'SELECT id FROM {view}')]) == set([1, 2, 3, 4]))
            # Remove that special row 4
            cql.execute(f'DELETE v1 FROM {base} WHERE id = 4')
            nodetool.flush(cql, view)
            assert(set([row.id for row in cql.execute(f'SELECT id FROM {view}')]) == set([1, 2, 3]))
            # We now believe that empty value serialization/deserialization is correct

# This test is regression testing added after fixing:
# https://github.com/scylladb/scylladb/issues/16392 - the gist of the issue is that
# prepared statements on views are not invalidated when the base table changes.
def test_mv_prepared_statement_with_altered_base(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'id int PRIMARY KEY, v1 int') as base:
        with new_materialized_view(cql, table=base, select='*', pk='id', where='id IS NOT NULL') as view:
            base_query = cql.prepare(f"SELECT * FROM {base} WHERE id=?")
            view_query = cql.prepare(f"SELECT * FROM {view} WHERE id=?")
            cql.execute(f"INSERT INTO {base} (id,v1) VALUES (0,0)")
            assert cql.execute(base_query,[0]) == cql.execute(view_query,[0])
            cql.execute(f"ALTER TABLE {base} ADD (v2 int)")
            cql.execute(f"INSERT INTO {base} (id,v1,v2) VALUES (1,1,1)")
            assert list(cql.execute(base_query,[1])) == list(cql.execute(view_query,[1]))

# A reproducer for issue #17117:
# When a single base update generates many view updates to the same partition,
# instead of processing the entire huge partition at once Scylla processes the
# view updates in chunks of 100 rows each (max_rows_for_view_updates).
# We had a bug with *range tombstones* which were mis-counted for this limit,
# and moreover - could cause a chunk to end in the middle of a range
# tombstone, which causes the range tombstone in this case to be lost and not
# reach the view.
# This test is a simple reproducer for this case. Because IN are limited
# in size to max_clustering_key_restrictions_per_query (100), we use a
# BATCH in this test to generate more than 100 (max_rows_for_view_updates)
# view updates from just one mutation.
def test_many_range_tombstone_base_update(cql, test_keyspace):
    # This test inserts N rows and deletes all of them in one batch.
    N = 234
    # We need two clustering key columns in this test, so that deleting
    # each "WHERE c1=?" will cause a *range* tombstone - which is what
    # we want to reproduce in this test.
    with new_test_table(cql, test_keyspace, 'p int, c1 int, c2 int, primary key (p, c1, c2)') as table:
        # For simplicity, the view is identical to the base. This is good
        # enough and still reproduces the bug. Remember that range tombstones
        # on the base are not copied to the view as-is - they are translated
        # to row tombstones in the view for the specific rows that really
        # exist in the base table.
        with new_materialized_view(cql, table, '*', 'p, c1, c2', 'p is not null and c1 is not null and c2 is not null') as mv:
            insert = cql.prepare(f'INSERT INTO {table} (p, c1, c2) VALUES (?,?,?)')
            # We need all of the rows to end up in the same view
            # partition, so all the deletions will be in the same
            # partition and will be divided into chunks. Hence we'll
            # use the same partition key 42 for all rows:
            p = 42
            for i in range(N):
                cql.execute(insert, [p, i, i])
            # Remove all N rows using N *range* tombstones (deleting based
            # on p,c1 but not c2), all in one write to the base (a batch):
            cmd = 'BEGIN BATCH '
            for i in range(N):
                cmd += f'DELETE FROM {table} WHERE p={p} AND c1={i} '
            cmd += 'APPLY BATCH;'
            cql.execute(cmd)
            # At this point, both base table and view tables should be
            # empty.
            assert [] == list(cql.execute(f'SELECT c1 FROM {table}'))
            assert [] == list(cql.execute(f'SELECT c1 FROM {mv}'))

# Another more elaborate reproducer for issue #17117, which is closer to the
# original use where we encountered this bug. It uses IN instead of BATCH, so
# it it is limited to deletions of max_clustering_key_restrictions_per_query
# (100) clustering ranges, but that's enough to reproduce this bug because
# anything more than 25 reproduced it. The view in this reproducer is also
# a bit more interesting than in the previous test (the view is not identical
# to the base, rather it combines several base partitions into one
# view partition).
def test_many_range_tombstone_base_update_2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, c1 int, c2 int, v1 int, v2 int, primary key ((p1,p2),c1,c2)') as table:
        with new_materialized_view(cql, table, '*', '(v1,p2),c1,p1,c2', 'v1 is not null and p2 is not null and c1 is not null and p1 is not null and c2 is not null') as mv:
            insert = cql.prepare(f'INSERT INTO {table} (p1,p2,c1,c2,v1,v2) VALUES (?,?,?,?,?,?)')
            # Insert N items, with:
            #    * p1 cycles between NP1 different values.
            #    * c1 is unique per item.
            #    * p2, c2, v1, and v2, are the same for all items.
            N = 500
            NP1 = 3
            # fixed values:
            p2 = 123
            v1 = 456
            v2 = 678
            c2 = 987
            for i in range(N):
                p1 = i % NP1
                c1 = i
                cql.execute(insert, [p1,p2,c1,c2,v1,v2])
            # Delete slice with prefix p1,p2,c1 for multiple c1's (any c2)
            delete_slices = cql.prepare(f'DELETE FROM {table} WHERE p1=? AND p2=? AND c1 in ?')
            # This test appears fail due to #17117 for any K>25 - the 26th
            # and every multiple of 26th deletion in the batch doesn't reach
            # the view.
            K=80
            for p1 in range(NP1):
                # c1's for this p1 are i's such that i%NP1 = p1.
                # Only take the c1's that are after N//2, to delete
                # only the later half of the items.
                start = N//2
                start -= start % NP1
                c1s = range(start + p1, N, NP1)
                # split c1s into chunks of length K
                chunks = []
                for x in range(0, len(c1s), K):
                    slice_item = slice(x, x + K, 1)
                    chunks.append(c1s[slice_item])
                for chunk in chunks:
                    cql.execute(delete_slices, [p1, p2, chunk])
            # The deletions above are pretty hard to follow, but no matter
            # what we deleted above, it should have been deleted from
            # both base and view. If the base and view differ, we have a bug.
            list_base = sorted([x.c1 for x in cql.execute(f"SELECT c1 FROM {table}")])
            list_view = sorted([x.c1 for x in cql.execute(f"SELECT c1 FROM {mv}")])
            print("Remaining base rows: ", len(list_base))
            print("Remaining base rows: ", len(list_view))
            print("Only in base: ", sorted(list(set(list_base)-set(list_view))))
            print("Only in view: ", sorted(list(set(list_view)-set(list_base))))
            assert list_base == list_view

# Test that deleting a base partition works fine, even if it produces a
# large batch of individual view updates. After issue #8852 was fixed,
# this large batch is no longer done together, but rather split to smaller
# batches, and this split can be done wrongly (e.g., see issue #17117)
# and we want to confirm that all the deletions are actually done.
#
# We have the related test for secondary indexes (test_secondary_index.py::
# test_partition_deletion), but this one uses materialized views directly
# instead of the secondary-index wrapper, and works on Cassandra as well.
# This test also exercises a more difficult scenario, where all view
# deletions end up in the same view partition, so the code is "tempted" to
# keep them all in the same output mutation and needs to break up this
# output mutation correctly (the test doesn't check that this breaking up
# happens, but rather that if it happens - it doesn't break correctness).
def test_base_partition_deletion(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p,c)') as table:
        # All inserts go to the same base partition,  that we'll then delete
        p = 1
        v = 42
        insert = cql.prepare(f'INSERT INTO {table} (p,c,v) VALUES ({p},?,{v})')
        # Case where all view-row deletions go to the same view partition:
        with new_materialized_view(cql, table, '*', 'p,v,c', 'p is not null and v is not null and c is not null') as mv:
            N = 345
            for i in range(N):
                cql.execute(insert, [i])
            # Before the deletion, all N rows should exist in the base and the
            # view
            allN = list(range(N))
            assert allN == [x.c for x in cql.execute(f"SELECT c FROM {table}")]
            assert allN == sorted([x.c for x in cql.execute(f"SELECT c FROM {mv}")])
            cql.execute(f"DELETE FROM {table} WHERE p=1")
            # After the deletion, all data should be gone from both base and view
            assert [] == list(cql.execute(f"SELECT c FROM {table}"))
            assert [] == list(cql.execute(f"SELECT c FROM {mv}"))

# Same as above test, just for a range tombstone, e.g., in a composite
# clustering key c1,c2 deleting in the base all rows with some c1.
# Here too Scylla generates a long list of view updates (individual row
# deletions), and if it's split into smaller batches, this needs to be
# done correctly and no view update missed.
# This test is related to issue #17117 - it doesn't reproduce that issue
# (we have reproducers for it above), but it's important to confirm that
# after fixing that issue, we don't break this case and can still split
# a large clustering prefix deletion into multiple batches without losing
# any view deletions.
def test_base_clustering_prefix_deletion(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c1 int, c2 int, v int, primary key (p,c1,c2)') as table:
        # All inserts go to the same base c1, that we'll then delete
        p = 1
        c1 = 2
        v = 42
        insert = cql.prepare(f'INSERT INTO {table} (p,c1,c2,v) VALUES ({p},{c1},?,{v})')
        # Case where all view-row deletions go to the same view partition:
        with new_materialized_view(cql, table, '*', 'p,v,c1,c2', 'p is not null and v is not null and c1 is not null and c2 is not null') as mv:
            N = 345
            for i in range(N):
                cql.execute(insert, [i])
            # Before the deletion, all N rows should exist in the base and the
            # view
            allN = list(range(N))
            assert allN == [x.c2 for x in cql.execute(f"SELECT c2 FROM {table}")]
            assert allN == sorted([x.c2 for x in cql.execute(f"SELECT c2 FROM {mv}")])
            cql.execute(f"DELETE FROM {table} WHERE p=1")
            # After the deletion, all data should be gone from both base and view
            assert [] == list(cql.execute(f"SELECT c2 FROM {table}"))
            assert [] == list(cql.execute(f"SELECT c2 FROM {mv}"))

# Test deleting an entire base partition, where there is a view with the same or
# different partition key.  When the view has the same partition key as base,
# the partition deletion can be done on the base in one update of partition
# tombstone.  In other cases, a tombstone is generated for each row that
# corresponds to the deleted base partition.
def test_base_partition_deletion_with_same_view_partition_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key (p,c)') as table:
        # Insert into two base partitions. We will delete one of them
        v = 42
        insert = cql.prepare(f'INSERT INTO {table} (p,c,v) VALUES (?,?,{v})')
        # Create multiple views with different primary keys.
        # The first view has the same partition key as the base, so the entire partition will be deleted on the view as well.
        # The other views have different partition key than the base, so they will have individual rows removed.
        with new_materialized_view(cql, table, '*', 'p,c', 'p is not null and c is not null') as mv1, \
             new_materialized_view(cql, table, '*', 'c,p', 'p is not null and c is not null') as mv2, \
             new_materialized_view(cql, table, '*', '(p,c),v', 'p is not null and c is not null and v is not null') as mv3:
            N = 10
            for i in range(N):
                cql.execute(insert, [1, i])
                cql.execute(insert, [2, i])

            # Before the deletion, all N rows should exist in the base and the view
            allN = list(range(N))
            assert allN == [x.c for x in cql.execute(f"SELECT c FROM {table} WHERE p=1")]
            assert allN == sorted([x.c for x in cql.execute(f"SELECT c FROM {mv1} WHERE p=1")])
            assert allN == [x.c for x in cql.execute(f"SELECT c FROM {table} WHERE p=2")]
            assert allN == sorted([x.c for x in cql.execute(f"SELECT c FROM {mv1} WHERE p=2")])
            for i in range(N):
                assert [1,2] == sorted([x.p for x in cql.execute(f"SELECT p FROM {mv2} WHERE c={i}")])

            cql.execute(f"DELETE FROM {table} WHERE p=1")

            # After the deletion, all data should be gone from both base and view
            assert [] == list(cql.execute(f"SELECT c FROM {table} WHERE p=1"))
            assert [] == list(cql.execute(f"SELECT c FROM {mv1} WHERE p=1"))
            assert [] == list(cql.execute(f"SELECT c FROM {mv2} WHERE p=1 ALLOW FILTERING"))
            assert [] == list(cql.execute(f"SELECT c FROM {mv3} WHERE p=1 ALLOW FILTERING"))

            assert allN == [x.c for x in cql.execute(f"SELECT c FROM {table} WHERE p=2")]
            assert allN == sorted([x.c for x in cql.execute(f"SELECT c FROM {mv1} WHERE p=2")])

            for i in range(N):
                assert [2] == sorted([x.p for x in cql.execute(f"SELECT p FROM {mv2} WHERE c={i}")])

            for i in range(N):
                assert [v] == sorted([x.v for x in cql.execute(f"SELECT v FROM {mv3} WHERE p=2 AND c={i}")])

# The partition key of the view is strictly contained in the base partition key,
# so multiple base partitions are combined into one view partition.
# Test deleting a base partition and verify it is deleted correctly in the view.
def test_base_partition_deletion_with_smaller_view_partition_key(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, c int, primary key ((p1,p2),c)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (p1,p2,c) VALUES (?,?,?)')
        with new_materialized_view(cql, table, '*', 'p1,p2,c', 'p1 is not null and p2 is not null and c is not null') as mv:
            # Insert into two separate base partitions.
            # In the view, the rows have the same partition key.
            cql.execute(insert, [0, 0, 10])
            cql.execute(insert, [0, 1, 20])

            # Delete one of the partitions
            cql.execute(f"DELETE FROM {table} WHERE p1=0 AND p2=0")

            assert [] == list(cql.execute(f"SELECT c FROM {table} WHERE p1=0 AND p2=0"))
            assert [20] == [x.c for x in cql.execute(f"SELECT c FROM {table} WHERE p1=0 AND p2=1")]

            assert [(1,20)] == [(x.p2, x.c) for x in cql.execute(f"SELECT p2, c FROM {mv} WHERE p1=0")]

# Test deleting a large partition when there is a view with the same partition
# key, and verify that view updates metrics is increased by exactly 1. Deleting
# a partition in this case is expected to generate one view update for deleting
# the corresponding view partition by a partition tombstone.
# Reproduces #8199
@pytest.mark.parametrize("permuted", [False, True])
def test_base_partition_deletion_with_metrics(cql, test_keyspace, scylla_only, permuted):
    with new_test_table(cql, test_keyspace, 'p1 int, p2 int, c int, primary key ((p1,p2),c)') as table:
        # Insert into one base partition. We will delete the entire partition
        insert = cql.prepare(f'INSERT INTO {table} (p1,p2,c) VALUES (?,?,?)')
        # The view partition key is a permutation of the base partition key.
        with new_materialized_view(cql, table, '*', '(p2,p1),c' if permuted else '(p1,p2),c', 'p1 is not null and p2 is not null and c is not null') as mv:
            # the metric total_view_updates_pushed_local is incremented by 1 for each 100 row view
            # updates, because it is collected in batches according to max_rows_for_view_updates.
            # To verify the behavior, we want the metric to increase by at least 2 without the optimization,
            # so 101 is the minimum value that works. With the optimization, we expect to have exactly 1 update
            # for any N.
            N = 101

            # all operations are on this single partition
            p1, p2 = 1, 10
            where_clause_table = f"WHERE p1={p1} AND p2={p2}"
            where_clause_mv = f"WHERE p2={p2} AND p1={p1}" if permuted else where_clause_table

            for i in range(N):
                cql.execute(insert, [p1, p2, i])

            # Before the deletion, all N rows should exist in the base and the view
            allN = list(range(N))
            assert allN == [x.c for x in cql.execute(f"SELECT c FROM {table} {where_clause_table}")]
            assert allN == sorted([x.c for x in cql.execute(f"SELECT c FROM {mv} {where_clause_mv}")])

            metrics_before = ScyllaMetrics.query(cql)
            updates_before = metrics_before.get('scylla_database_total_view_updates_pushed_local')

            cql.execute(f"DELETE FROM {table} {where_clause_table}")

            # After the deletion, all data should be gone from both base and view
            assert [] == list(cql.execute(f"SELECT c FROM {table} {where_clause_table}"))
            assert [] == list(cql.execute(f"SELECT c FROM {mv} {where_clause_mv}"))

            metrics_after = ScyllaMetrics.query(cql)
            updates_after = metrics_after.get('scylla_database_total_view_updates_pushed_local')

            print(f"scylla_database_total_view_updates_pushed_local: {updates_before} -> {updates_after}")
            assert updates_after == updates_before + 1

# Perform a batch operation, deleting a partition and also inserting a row
# to that partition with a newer timestamp, and verify that the insertion
# is not lost in the MV update.
def test_base_partition_deletion_in_batch_with_insert(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, primary key (p,c)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (p,c) VALUES (?,?) USING TIMESTAMP 99')
        with new_materialized_view(cql, table, '*', 'p,c', 'p is not null and c is not null') as mv:
            cql.execute(insert, [0, 1])
            cql.execute(insert, [0, 2])
            cql.execute(insert, [0, 3])

            # This should delete all the existing partition rows, and the new
            # row insertion survives and remains the only row after the operation
            # since it has the most recent timestamp.
            cmd = 'BEGIN UNLOGGED BATCH '
            cmd += f'INSERT INTO {table} (p,c) VALUES (0,4) USING TIMESTAMP 98; '
            cmd += f'DELETE FROM {table} USING TIMESTAMP 100 WHERE p=0; '
            cmd += f'INSERT INTO {table} (p,c) VALUES (0,5) USING TIMESTAMP 101; '
            cmd += 'APPLY BATCH;'
            cql.execute(cmd)

            # Verify it is correct both in the table and the view
            assert [5] == [x.c for x in cql.execute(f"SELECT c FROM {table} WHERE p=0")]
            assert [5] == [x.c for x in cql.execute(f"SELECT c FROM {mv} WHERE p=0")]

# Similar to the test above, perform a deletion of a base partition in a batch with
# deletion of individual rows. Verify the partition is deleted correctly and that
# a single update is generated for the view for deleting the whole partition, and no
# view updates for each row.
def test_base_partition_deletion_in_batch_with_delete_row_with_metrics(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c int, v int, primary key ((p,c),v)') as table:
        insert = cql.prepare(f'INSERT INTO {table} (p,c,v) VALUES (?,?,?)')
        # The view partition key is the same as the base partition key.
        with new_materialized_view(cql, table, '*', '(p,c),v', 'p is not null and c is not null and v is not null') as mv:
            N = 101 # See comment above
            for i in range(N):
                cql.execute(insert, [1, 10, i])

            # Before the deletion, all N rows should exist in the base and the view
            allN = list(range(N))
            assert allN == [x.v for x in cql.execute(f"SELECT v FROM {table} WHERE p=1 AND c=10")]
            assert allN == sorted([x.v for x in cql.execute(f"SELECT v FROM {mv} WHERE p=1 AND c=10")])

            metrics_before = ScyllaMetrics.query(cql)
            updates_before = metrics_before.get('scylla_database_total_view_updates_pushed_local')

            # The batch deletes the entire partition and also, redundantly, deleting individual rows in the partition.
            # We expect the view update to contain only a single update for deleting the partition.
            cmd = 'BEGIN UNLOGGED BATCH '
            for i in range(100,500):
                cmd += f'DELETE FROM {table} WHERE p=1 AND c=10 AND v={i}; '
            cmd += f'DELETE FROM {table} WHERE p=1 AND c=10; '
            cmd += 'APPLY BATCH;'
            cql.execute(cmd)

            # Verify the partition is deleted
            assert [] == list(cql.execute(f"SELECT v FROM {table} WHERE p=1 AND c=10"))
            assert [] == list(cql.execute(f"SELECT v FROM {mv} WHERE p=1 AND c=10"))

            # Verify there is a single view update
            metrics_after = ScyllaMetrics.query(cql)
            updates_after = metrics_after.get('scylla_database_total_view_updates_pushed_local')

            print(f"scylla_database_total_view_updates_pushed_local: {updates_before} -> {updates_after}")
            assert updates_after == updates_before + 1

# Delete a base partition using a timestamp lower than some of the rows
# in the partition. Verify it doesn't result new delete in the base
# and the view partition.
def test_base_partition_deletion_with_low_timestamp(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, primary key (p,c)') as table:
        with new_materialized_view(cql, table, '*', 'p,c', 'p is not null and c is not null') as mv:
            cql.execute(f'INSERT INTO {table} (p,c) VALUES (0,1) USING TIMESTAMP 99')
            cql.execute(f'INSERT INTO {table} (p,c) VALUES (0,2) USING TIMESTAMP 101')

            # Delete the partition with a timestamp which is older than some of
            # the rows and newer than other rows.
            cql.execute(f'DELETE FROM {table} USING TIMESTAMP 100 WHERE p=0')

            # Verify we get only the row with the newer timestamp
            assert [2] == [x.c for x in cql.execute(f"SELECT c FROM {table} WHERE p=0")]
            assert [2] == [x.c for x in cql.execute(f"SELECT c FROM {mv} WHERE p=0")]
