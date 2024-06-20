# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# The cql-pytest framework is about testing CQL functionality, so
# implementation details like sstables cannot be tested directly. However,
# we are still able to reproduce some bugs by tricks such as writing some
# data to the table and then force it to be written to the disk (nodetool
# flush) and then trying to read the data again, knowing it must come from
# the disk.
#############################################################################

from util import new_test_table
import nodetool
import random

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

# Reproduces https://github.com/scylladb/scylladb/issues/17625.
# Without the fix, it trips ASAN.
def test_big_key_index_reader(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "pk blob, ck blob, PRIMARY KEY (pk, ck)") as table:
        # Insert a partition with a partition key large enough to be fragmented in LSA,
        # and enough clustering rows for a promoted index to be written.
        insert = cql.prepare(f"INSERT INTO {table}(pk, ck) VALUES(?, ?)")
        k = 30000
        pk = random.randbytes(k)
        for i in range(10):
            ck = random.randbytes(k)
            cql.execute(insert, [pk, ck])
        # Flush the partition.
        nodetool.flush(cql, table)
        # Read the partition from sstables.
        select = cql.prepare(f"SELECT pk, ck FROM {table} WHERE pk = ? BYPASS CACHE")
        cql.execute(select, [pk])
