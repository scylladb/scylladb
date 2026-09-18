# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for the restrictions a strongly consistent table puts on a statement.
#
# A strongly consistent statement is executed by a single raft group, the one
# owning the tablet of the partition it touches, which is why the coordinator
# rejects anything that does not resolve to exactly one partition, and anything
# mixing a strongly consistent table with an eventually consistent one in a
# single batch.  Each rejection happens on the coordinator before the raft
# group is ever contacted, so a single node and plain CQL are enough - see the
# header of test_strong_consistency.py for the split against test/cluster.
#
# The rejections themselves live in:
#   cql3/statements/strong_consistency/select_statement.cc       (reads)
#   cql3/statements/strong_consistency/modification_statement.cc (writes)
#   transport/server.cc                                          (mixed batch)
#
# Restrictions which apply to a batch as a whole - a batch mixing keyspaces in
# its textual form, batch level USING TIMESTAMP and USING TTL, consistency
# levels, counters - are tested next to the rest of the batch behaviour in
# test_strong_consistency.py.
#############################################################################

import pytest
from cassandra.protocol import InvalidRequest
from cassandra.query import BatchStatement, BatchType

from ..util import new_test_table

# The sc_keyspace fixture lives in conftest.py.

# Raised by both the read and the write path for a statement that does not
# resolve to exactly one partition.
ERR_SINGLE_PARTITION = "Strongly consistent queries can only target a single partition"
# Raised by the native protocol batch path for a batch which is part strongly
# consistent and part eventually consistent.
ERR_CANNOT_MIX = "Cannot mix strongly consistent and eventually consistent statements in a batch"


# Every shape of read which does not resolve to a single partition goes through
# the same check, so one representative of each is enough: a full scan, a scan
# with an aggregate on top, several single-partition ranges, and a token range.
# The aggregate is worth a case of its own because the strongly consistent
# branch of select_statement::prepare() is picked before can_be_mapreduced(),
# so a count(*) never reaches the parallelized aggregation path which would
# have fanned the query out to every shard of every replica.  An
# ALLOW FILTERING query is the same full scan and is not repeated here.
def test_reject_multi_partition_reads(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 10)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (2, 1, 20)")

        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT * FROM {table}")
        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT count(*) FROM {table}")
        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT * FROM {table} WHERE pk IN (1, 2)")
        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"SELECT * FROM {table} WHERE token(pk) > 0")

        # A single partition, however the restriction spells it, is fine - an
        # IN with one element resolves to one range like a plain equality does.
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 1")) == [(1, 1, 10)]
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk IN (2)")) == [(2, 1, 20)]


# The write path builds its partition keys and applies the same check, so a
# DELETE spanning two partitions is rejected.  An UPDATE with the same IN
# restriction shares build_partition_keys() and is not repeated here.  The same
# DELETE inside a batch is covered by test_batch; this is the standalone
# statement, which reaches the check through modification_statement rather than
# through the batch.
def test_reject_multi_partition_writes(cql, sc_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table:
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, 1, 10)")
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (2, 1, 20)")

        with pytest.raises(InvalidRequest, match=ERR_SINGLE_PARTITION):
            cql.execute(f"DELETE FROM {table} WHERE pk IN (1, 2) AND ck = 1")

        # A single partition, however the restriction spells it: the check
        # counts the keys the statement builds, so a one element IN is as good
        # as an equality - and the delete takes that row and nothing else.
        cql.execute(f"DELETE FROM {table} WHERE pk IN (2) AND ck = 1")
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 2")) == []
        assert list(cql.execute(f"SELECT pk, ck, v FROM {table} WHERE pk = 1")) == [(1, 1, 10)]


# A native protocol batch carries prepared statements which the server has
# already classified, so it can tell a mixed batch apart from a homogeneous one
# and rejects it.  The check counts the strongly consistent statements against
# the batch size, so the order the two halves arrive in does not matter.  The
# textual form of the same batch is rejected elsewhere, by
# batch_statement::prepare(), and is covered by
# test_mixed_keyspace_batch_on_sc_table.
def test_reject_mixed_batch_native_protocol(cql, sc_keyspace, test_keyspace):
    with new_test_table(cql, sc_keyspace, "pk int PRIMARY KEY, v int") as sc_table, \
         new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as ec_table:
        sc_insert = cql.prepare(f"INSERT INTO {sc_table} (pk, v) VALUES (?, ?)")
        ec_insert = cql.prepare(f"INSERT INTO {ec_table} (pk, v) VALUES (?, ?)")

        for first, second in [(sc_insert, ec_insert), (ec_insert, sc_insert)]:
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.add(first, (1, 10))
            batch.add(second, (1, 20))
            with pytest.raises(InvalidRequest, match=ERR_CANNOT_MIX):
                cql.execute(batch)

        # Neither half of a rejected batch may be applied.
        assert list(cql.execute(f"SELECT pk, v FROM {sc_table} WHERE pk = 1")) == []
        assert list(cql.execute(f"SELECT pk, v FROM {ec_table} WHERE pk = 1")) == []
