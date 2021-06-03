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
# The cql-pytest framework is about testing CQL functionality, so
# implementation details like sstables cannot be tested directly. However,
# we are still able to reproduce some bugs by tricks such as writing some
# data to the table and then force it to be written to the disk (nodetool
# flush) and then trying to read the data again, knowing it must come from
# the disk.
#############################################################################

import pytest
from util import unique_name, new_test_table
import nodetool

# Reproduces issue #8138, where the sstable reader in a TWCS sstable set
# had a bug and resulted in no results for queries.
# This is a Scylla-only test because it uses BYPASS CACHE which does
# not exist on Cassandra.
def test_twcs_optimal_query_path(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace,
        "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
        " WITH COMPACTION = {" +
        " 'compaction_window_size': '1'," +
        " 'compaction_window_unit': 'MINUTES'," +
        " 'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy' }") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (0, 0, 0)")
        # Obviously, scanning the table should now return exactly one row:
        assert 1 == len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 0")))
        # We will now flush the memtable to disk, and execute the same
        # query again with BYPASS CACHE, to be sure to exercise the code that
        # reads from sstables. We will obviously expect to see the same one
        # result. Issue #8138 caused here zero results, as well as a crash
        # in the debug build.
        nodetool.flush(cql, table)
        assert 1 == len(list(cql.execute(f"SELECT * FROM {table} WHERE pk = 0 BYPASS CACHE")))
