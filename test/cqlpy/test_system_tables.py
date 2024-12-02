# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for the content of system tables. Many of these tables have
# content that was defined by Cassandra, and applications and driver assume,
# or may assume, that Scylla provides similar content.
#############################################################################

from util import new_test_table
import pytest
import nodetool

#############################################################################
# system.size_estimates.partitions_count
# Provides an estimate for the number of partitions in a table. A node
# publishes separate estimates per *primary range* it owns (i.e., a vnode that
# it is its primary replica). This allows to easily and efficiently sum up
# the counts received from all nodes in a DC to get an estimated total number
# of partitions across the entire DC.
#############################################################################

# The test_partitions_estimate_simple_* tests below look at just the
# simplest case: we write N different partitions to a table, and look at how
# close the partition count estimate is to the truth.

# Utility function creating a temporary table, writing N partitions into
# it and then returning the total size_estimates.partitions_count for this
# table:
def write_table_and_estimate_partitions(cql, test_keyspace, N):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        write = cql.prepare(f"INSERT INTO {table} (k) VALUES (?)")
        for i in range(N):
            cql.execute(write, [i])
        # Both Cassandra and Scylla do not include memtable data in their
        # estimates, so a nodetool.flush() is required to get a count.
        nodetool.flush(cql, table)
        # In Cassandra, the estimates may not be available until a
        # nodetool.refreshsizeestimates(). In Scylla it is not needed.
        nodetool.refreshsizeestimates(cql)
        # The size_estimates table has, for a keyspace/table partition, a
        # separate row for separate token ranges. We need to sum those up.
        table_name = table[len(test_keyspace)+1:]
        counts = [x.partitions_count for x in cql.execute(
            f"SELECT partitions_count FROM system.size_estimates WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'")]
        count = sum(counts)
        print(counts)
        print(count)
        return count

# We expect that when write_table_and_estimate_partitions writes N partitions
# and returns Scylla's or Cassandra's estimate on the number of partitions,
# this estimate would be *around* N. However, we don't know how close it should
# be to N. Experimentally, in Cassandra the accuracy is better for larger
# tables, i.e. the error is larger for smaller tables. The following is a small
# and quick test, with N=1000. Experimentally, for N=897 through N=1024,
# Cassandra returns same estimate 1024 - so the inaccuracy of the estimate is
# up to 14%. So just to be generous let's allow a 25% inaccuracy for this
# small test. In issue #9083 we noted that Scylla had much larger errors -
# reporting as much as 10880 (!) partitions when we have just 1000.
@pytest.mark.xfail(reason="issue #9083")
def test_partitions_estimate_simple_small(cql, test_keyspace):
    N = 1000
    count = write_table_and_estimate_partitions(cql, test_keyspace, N)
    assert count > N/1.25 and count < N*1.25

# For a larger test, the estimation accuracy should be better:
# Experimentally, for 10,000 rows, Cassandra's estimation error goes
# down to just 1.3%. Let's be generous and allow a 5% inaccuracy:
# This is a relatively long test (takes around 2 seconds), and isn't
# needed to reproduce #9083 (the previous shorter test does it too),
# so we skip this test.
@pytest.mark.xfail(reason="issue #9083")
@pytest.mark.skip(reason="slow test, remove skip to try it anyway")
def test_partitions_estimate_simple_large(cql, test_keyspace):
    N = 10000
    count = write_table_and_estimate_partitions(cql, test_keyspace, N)
    assert count > N/1.05 and count < N*1.05

# If we write the *same* 1000 partitions to two sstables (by flushing twice,
# and assuming that 1000 tiny partitions easily fit a memtable), and check
# if the partition estimate, it should *not* return double the accurate count
# just because it naively sums up the estimates for the different sstables.
# Rather it should use the cardinality estimator to estimate the overlap.
# Currently both Cassandra and Scylla fail this test. They are simply not
# meant to provide accurate partition-count estimates when faced with high
# space amplification.
@pytest.mark.xfail(reason="partition count estimator does not use cardinality estimator")
def test_partitions_estimate_full_overlap(cassandra_bug, cql, test_keyspace):
    N = 500
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        write = cql.prepare(f"INSERT INTO {table} (k) VALUES (?)")
        for i in range(N):
            cql.execute(write, [i])
        nodetool.flush(cql, table)
        # And a second copy of the *same* data will end up in a second sstable:
        for i in range(N):
            cql.execute(write, [i])
        nodetool.flush(cql, table)
        # TODO: In Scylla we should use NullCompactionStrategy to avoid the two
        # sstables from immediately being compacted together.
        nodetool.refreshsizeestimates(cql)
        table_name = table[len(test_keyspace)+1:]
        counts = [x.partitions_count for x in cql.execute(
            f"SELECT partitions_count FROM system.size_estimates WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'")]
        count = sum(counts)
        print(counts)
        print(count)
        assert count > N/1.5 and count < N*1.5

# Test that deleted partitions should not be counted by the estimated
# partitions count. Unfortunately, the current state of both Cassandra
# and Scylla is that they *are* counted.
# This is the simplest test involving deletions: we *only* delete partitions
# (there are no insertions at all), so the database has no live partitions
# at all, just tombstones - yet the count returns the number of these
# tombstones.
@pytest.mark.xfail(reason="partition count estimator doesn't handle deletions")
def test_partitions_estimate_only_deletions(cassandra_bug, cql, test_keyspace):
    N = 1000
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        delete = cql.prepare(f"DELETE FROM {table} WHERE k=?")
        for i in range(N):
            cql.execute(delete, [i])
        nodetool.flush(cql, table)
        nodetool.refreshsizeestimates(cql)
        table_name = table[len(test_keyspace)+1:]
        counts = [x.partitions_count for x in cql.execute(
            f"SELECT partitions_count FROM system.size_estimates WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'")]
        count = sum(counts)
        print(counts)
        print(count)
        # Count should be close to 0, not to N
        assert count < N/1.25
