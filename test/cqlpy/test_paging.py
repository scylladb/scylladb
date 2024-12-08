# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from util import new_test_table
from cassandra.query import SimpleStatement
import pytest
import nodetool

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
