# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from .util import new_test_table
from cassandra.query import SimpleStatement
import pytest
from . import nodetool

# Test that the _stop flag set in the compactor at the end of a page is not
# sticky and doesn't remain set on the following page. If it does it can cause
# the next page (and consequently the entire query) to be terminated prematurely.
# This can happen if the code path on the very first consumed fragment doesn't
# reset this flag. Currently this is the case for rows completely covered by a
# higher level tombstone.
def test_sticky_stop_flag(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)') as table:
        insert_row_id = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")

        pk = 0

        # Flush the row to disk, to prevent it being compacted away in the
        # memtable upon writing the partition tombstone.
        cql.execute(insert_row_id, (pk, 100, 0))
        nodetool.flush(cql, table)
        cql.execute(f"DELETE FROM {table} WHERE pk = {pk}")

        for ck in range(0, 200):
            if ck == 100:
                continue
            cql.execute(insert_row_id, (pk, ck, 0))

        statement = SimpleStatement(f"SELECT * FROM {table} WHERE pk = {pk}", fetch_size=100)

        res = list(cql.execute(statement))

        assert len(res) == 199

# In this test we verify that setting page_size=0 truly disables paging in
# a request:
# Scylla's paging not only ends pages after a query-defined "page_size" rows,
# it also ends a page when reaching (roughly) a predefined size cutoff - 1MB.
# In this test we verify that setting page_size = 0 truly disables paging,
# i.e., it also disables that size cutoff, and the single page of response
# can grow beyond 1MB.
#
# This is a Scylla-only test because Cassandra cuts pages only by row counts,
# so if rows are long we can end up with extremely large pages in the
# response, even when paging is enabled.
def test_page_size_0(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'p int, c int, s text, PRIMARY KEY (p, c)') as table:
        # We want the data below to span more than the 1MB page size
        # cutoff, so choose long_len * nrows > 1MB.
        long_len = 100000
        nrows = 15
        stmt = cql.prepare(f'INSERT INTO {table} (p, c, s) VALUES (?, ?, ?)')
        p = 0
        long = 'x'*long_len
        for c in range(nrows):
            cql.execute(stmt, [p, c, long])
        # Reading with fetch_size = 3 should return a page with just three rows.
        r = cql.execute(SimpleStatement(f'SELECT * FROM {table} WHERE p={p}', fetch_size=3))
        assert len(r.current_rows) == 3
        # Reading with fetch_size = 100000 will not return all rows - it will
        # stop after the 1MB cutoff, while returning all the rows would have
        # taken more than 1MB. So there will be fewer results than nrows.
        r = cql.execute(SimpleStatement(f'SELECT * FROM {table} WHERE p={p}', fetch_size=100000))
        assert len(r.current_rows) < nrows
        # If we set fetch_size = 0, paging is completely disabled so the query
        # doesn't stop after reaching 1MB and we get all nrows results:
        r = cql.execute(SimpleStatement(f'SELECT * FROM {table} WHERE p={p}', fetch_size=0))
        assert len(r.current_rows) == nrows
        # Since commit 08c81427b9e we use page_size = -1 for internal unpaged
        # queries. Let's verify that page_size = -1 also means unpaged.
        # However, we don't recommend that users actually use page_size = -1,
        # it's better to stick to the traditional page_size = 0.
        r = cql.execute(SimpleStatement(f'SELECT * FROM {table} WHERE p={p}', fetch_size=-1))
        assert len(r.current_rows) == nrows
