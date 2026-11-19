# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# The cqlpy test framework is about testing CQL functionality, so
# implementation details like sstables cannot be tested directly. However,
# we are still able to reproduce some bugs by tricks such as writing some
# data to the table and then force it to be written to the disk (nodetool
# flush) and then trying to read the data again, knowing it must come from
# the disk.
#############################################################################

from .util import new_test_table
from . import nodetool
import math
import random

import pytest

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

# The row index of an sstable in the "ms" or "mt" format is a trie of
# clustering keys in a byte-comparable form, which must be ordered like the
# keys themselves. A float or a double is stored with the exact bits that the
# client sent, so a stored NaN can have its sign bit set - which is the NaN
# that arithmetic (e.g., 0.0 / 0.0) produces on x86. Such a NaN is ordered,
# like any other NaN, after all the other values, but its byte-comparable form
# used to be ordered before all of them. So when a partition had such a NaN as
# a clustering key and was large enough to have a row index, writing its
# sstable aborted on a failed assertion in the trie writer, or wrote an index
# in which the NaN's row could not be found - with the keys of this test, the
# former with an ascending clustering order, the latter with a descending one.
# Also, looking such a NaN up in a partition which had the usual NaN, without
# the sign bit, did not find its row, although all NaNs are equal.
# This is a Scylla-only test because it uses BYPASS CACHE, which does not
# exist on Cassandra.
@pytest.mark.parametrize("order", ["DESC", "ASC"])
@pytest.mark.parametrize("ck_type,nan_with_sign_bit,usual_nan",
        [("float", "ffc00000", "7fc00000"), ("double", "fff8000000000000", "7ff8000000000000")])
def test_nan_with_sign_bit_clustering_key(cql, test_keyspace, scylla_only, ck_type, nan_with_sign_bit, usual_nan, order):
    with new_test_table(cql, test_keyspace, f"pk int, ck {ck_type}, v text, PRIMARY KEY (pk, ck)",
            f"WITH CLUSTERING ORDER BY (ck {order})") as table:
        # A row index entry is written for every column_index_size_in_kb
        # (64 KB by default) of rows, so each of the following rows gets one.
        v = 'x' * 70000
        numbers = [float('-inf'), -1.0, 0.0, 1.0, float('inf')]
        # blobAsFloat() and blobAsDouble() give us a NaN with exactly the bits
        # we want, whatever the client driver does with a NaN. Partition 0
        # has the NaN with the sign bit, and partition 1 has the usual one.
        nans = [bytes.fromhex(nan_with_sign_bit), bytes.fromhex(usual_nan)]
        insert = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, ?, ?)")
        insert_nan = cql.prepare(f"INSERT INTO {table} (pk, ck, v) VALUES (?, blobAs{ck_type}(?), ?)")
        for pk, nan in enumerate(nans):
            for ck in numbers:
                cql.execute(insert, [pk, ck, v])
            cql.execute(insert_nan, [pk, nan, v])
        nodetool.flush(cql, table)
        select = cql.prepare(f"SELECT ck FROM {table} WHERE pk = ? AND ck = ? BYPASS CACHE")
        select_nan = cql.prepare(f"SELECT ck FROM {table} WHERE pk = ? AND ck = blobAs{ck_type}(?) BYPASS CACHE")
        for pk in range(len(nans)):
            # The NaN is ordered after all the numbers, including the infinity.
            cks = [row.ck for row in cql.execute(f"SELECT ck FROM {table} WHERE pk = {pk} BYPASS CACHE")]
            if order == "DESC":
                cks.reverse()
            assert cks[:-1] == numbers
            assert math.isnan(cks[-1])
            # Each row can be found through the index. All NaNs are equal, so
            # each of the two NaNs finds the row of the NaN in the partition.
            for ck in numbers:
                assert [row.ck for row in cql.execute(select, [pk, ck])] == [ck]
            for nan in nans:
                cks = [row.ck for row in cql.execute(select_nan, [pk, nan])]
                assert len(cks) == 1 and math.isnan(cks[0]), f"partition {pk}, looked up by the NaN 0x{nan.hex()}"
            # A slice which ends, or begins, with the NaN's row.
            cks = [row.ck for row in cql.execute(f"SELECT ck FROM {table} WHERE pk = {pk} AND ck > 1.0 BYPASS CACHE")]
            if order == "DESC":
                cks.reverse()
            assert len(cks) == 2 and cks[0] == float('inf') and math.isnan(cks[1])
