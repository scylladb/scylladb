# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for clients that use "synthetic" write timestamps: every write
# carries USING TIMESTAMP with a value that has no relationship to real time
# (a version counter starting at 1, a value far in the past or the future,
# or a negative number). Conflict resolution must depend only on the
# timestamps, and machinery that also runs on wall-clock time (tombstone
# deletion times, TTL expiry, tombstone garbage collection) must never
# compare wall-clock time against a write timestamp.
#
# Every test runs in several timestamp "eras", far apart from each other and
# from the current time. Within a test, timestamps are small offsets from the
# era's base.
#
# Cluster-level properties (read repair, row-level repair, incremental repair,
# tombstone GC after repair) are tested in test/cluster/test_synthetic_timestamps.py.
#############################################################################

import json
import time

import pytest

from . import nodetool
from .util import new_test_keyspace, new_test_table, unique_key_int, config_value_context, is_scylla


timestamp_eras = {
    # Control: ordinary wall-clock timestamps.
    'wall_clock': None,
    # A version counter starting at 1 - looks like a write from 1970.
    'counter': 1,
    # Negative timestamps are legal (only -2**63 is reserved).
    'negative': -2**62,
    # Two days into the future: still accepted with the default
    # restrict_future_timestamp=true. A placeholder; computed per test.
    'near_future': None,
    # Thousands of years into the future. Scylla rejects timestamps more
    # than 3 days into the future unless restrict_future_timestamp=false.
    'far_future': 2**62,
}


@pytest.fixture(params=timestamp_eras.keys())
def era(request, cql):
    base = timestamp_eras[request.param]
    if request.param == 'wall_clock':
        base = int(time.time() * 1_000_000)
    elif request.param == 'near_future':
        base = int((time.time() + 2 * 24 * 3600) * 1_000_000)
    if request.param == 'far_future' and is_scylla(cql):
        with config_value_context(cql, 'restrict_future_timestamp', 'false'):
            yield base
    else:
        yield base


@pytest.fixture(scope="module")
def table_no_gc(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, w int, PRIMARY KEY (pk, ck)",
                        " WITH tombstone_gc = {'mode': 'disabled'}") as table:
        yield table


# Tombstone GC also holds gc_before back to the time of the table's oldest
# write in any live commitlog segment, and segments stay alive while other
# tables have unflushed data in them. Bypass the commitlog so that purging
# depends only on the properties under test.
@pytest.fixture(scope="module")
def keyspace_without_commitlog(cql, scylla_only):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                                "AND durable_writes = false") as keyspace:
        yield keyspace


@pytest.fixture
def table_immediate_gc(cql, keyspace_without_commitlog):
    with new_test_table(cql, keyspace_without_commitlog, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                        " WITH tombstone_gc = {'mode': 'immediate'}"
                        # Keep each flush in its own sstable until the test compacts.
                        " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}") as table:
        yield table


def select_rows(cql, table, pk):
    return [(r.ck, r.v) for r in cql.execute(f"SELECT ck, v FROM {table} WHERE pk = {pk}")]


def sstable_fragments(cql, table, pk):
    """The mutation fragments of partition pk found in sstables of the
    (single) node, as (fragment kind, metadata) pairs."""
    rows = cql.execute(f"SELECT mutation_fragment_kind, metadata FROM MUTATION_FRAGMENTS({table}) "
                       f"WHERE pk = {pk} AND mutation_source > 'sstable:' AND mutation_source < 'sstable;'")
    return [(r.mutation_fragment_kind, json.loads(r.metadata) if r.metadata else None) for r in rows]


def wait_for_deletion_time_to_pass():
    """Tombstone deletion times have a resolution of one second, and a
    tombstone is purgeable only when its deletion time is strictly before
    gc_before, so make sure the current second has ended."""
    time.sleep(1.1)


# Writes arriving in arbitrary real-time order are resolved by their
# timestamps, both in the memtable and when merged across sstables by reads
# and by compaction.
writes = {
    'insert': "INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 0, {value}) USING TIMESTAMP {ts}",
    'update': "UPDATE {table} USING TIMESTAMP {ts} SET v = {value} WHERE pk = {pk} AND ck = 0",
}


@pytest.mark.parametrize("write", writes.keys())
def test_last_write_wins_by_timestamp_not_arrival(cql, table_no_gc, era, write):
    pk = unique_key_int()
    for offset, value in [(30, 3), (10, 1), (20, 2)]:
        cql.execute(writes[write].format(table=table_no_gc, pk=pk, value=value, ts=era + offset))
        assert select_rows(cql, table_no_gc, pk) == [(0, 3)]
        nodetool.flush(cql, table_no_gc)
        assert select_rows(cql, table_no_gc, pk) == [(0, 3)]
    nodetool.compact(cql, table_no_gc)
    assert select_rows(cql, table_no_gc, pk) == [(0, 3)]
    assert list(cql.execute(f"SELECT writetime(v) FROM {table_no_gc} WHERE pk = {pk} AND ck = 0")) == [(era + 30,)]


deletions = {
    'cell': "DELETE v FROM {table} USING TIMESTAMP {ts} WHERE pk = {pk} AND ck = 1",
    'row': "DELETE FROM {table} USING TIMESTAMP {ts} WHERE pk = {pk} AND ck = 1",
    'range': "DELETE FROM {table} USING TIMESTAMP {ts} WHERE pk = {pk} AND ck >= 0 AND ck <= 2",
    'partition': "DELETE FROM {table} USING TIMESTAMP {ts} WHERE pk = {pk}",
}


# A deletion shadows exactly the writes with a lower (or equal) timestamp,
# including writes that arrive after it in real time, and keeps doing so
# through flush and compaction.
@pytest.mark.parametrize("deletion", deletions.keys())
def test_deletion_shadows_by_timestamp_not_arrival(cql, table_no_gc, era, deletion):
    pk = unique_key_int()
    def visible():
        return [(r.ck, r.v) for r in cql.execute(f"SELECT ck, v FROM {table_no_gc} WHERE pk = {pk}") if r.v is not None]
    cql.execute(f"INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 1, 1) USING TIMESTAMP {era + 1}")
    nodetool.flush(cql, table_no_gc)
    cql.execute(deletions[deletion].format(table=table_no_gc, pk=pk, ts=era + 10))
    assert visible() == []
    nodetool.flush(cql, table_no_gc)
    # Arrives after the deletion, but is older.
    cql.execute(f"INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 1, 5) USING TIMESTAMP {era + 5}")
    # Same timestamp as the deletion: the deletion wins.
    cql.execute(f"INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 1, 10) USING TIMESTAMP {era + 10}")
    assert visible() == []
    nodetool.flush(cql, table_no_gc)
    assert visible() == []
    nodetool.compact(cql, table_no_gc)
    assert visible() == []
    # Newer than the deletion: visible.
    cql.execute(f"INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 1, 11) USING TIMESTAMP {era + 11}")
    assert visible() == [(1, 11)]
    nodetool.compact(cql, table_no_gc)
    assert visible() == [(1, 11)]


# The tombstone's deletion time, which drives tombstone GC, is taken from
# the wall clock when the deletion arrives, not derived from the write
# timestamp: a tombstone with a timestamp far in the past is not purged
# early (in a way that could resurrect data) just because of its timestamp,
# and one with a timestamp far in the future is not held forever.
@pytest.mark.parametrize("deletion", deletions.keys())
def test_tombstone_gc_purges_tombstone_and_shadowed_data(cql, table_immediate_gc, era, deletion):
    pk = unique_key_int()
    # UPDATE rather than INSERT, so a cell deletion leaves no row marker behind.
    cql.execute(f"UPDATE {table_immediate_gc} USING TIMESTAMP {era + 1} SET v = 1 WHERE pk = {pk} AND ck = 1")
    nodetool.flush(cql, table_immediate_gc)
    cql.execute(deletions[deletion].format(table=table_immediate_gc, pk=pk, ts=era + 10))
    nodetool.flush(cql, table_immediate_gc)
    fragments = sstable_fragments(cql, table_immediate_gc, pk)
    assert any(m and 'tombstone' in m for _, m in fragments) or any(k == 'range tombstone change' for k, _ in fragments)
    wait_for_deletion_time_to_pass()
    nodetool.compact(cql, table_immediate_gc)
    assert select_rows(cql, table_immediate_gc, pk) == []
    assert sstable_fragments(cql, table_immediate_gc, pk) == []


# Compaction must not purge a tombstone while data it shadows is outside the
# compaction (here: in the memtable). That data arrived after the tombstone
# but carries an older timestamp, so only a timestamp-based check protects it
# from being resurrected. The data arrives well within gc_grace_seconds of the
# deletion, so this is within the tombstone GC contract.
# Reproduces SCYLLADB-4755, which is not specific to synthetic timestamps
# (it happens with wall-clock timestamps too): get_fully_expired_sstables() drops an
# sstable holding only an expired tombstone without consulting the memtables.
# With a commitlog, the gc_before cap from commitlog segment times usually
# hides it. A cell deletion leaves a live row marker in the sstable, so that
# sstable is not fully expired and the memtable check does its job.
@pytest.mark.parametrize("deletion", [
    'cell',
    *[pytest.param(d, marks=pytest.mark.xfail(reason="SCYLLADB-4755: fully expired sstable detection ignores memtables", strict=True))
      for d in ('row', 'range', 'partition')],
])
def test_tombstone_gc_waits_for_shadowed_data_in_memtable(cql, keyspace_without_commitlog, era, deletion):
    gc_grace_seconds = 2
    with new_test_table(cql, keyspace_without_commitlog, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                        f" WITH tombstone_gc = {{'mode': 'timeout'}} AND gc_grace_seconds = {gc_grace_seconds}"
                        " AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}") as table:
        pk = unique_key_int()
        def visible():
            return [(r.ck, r.v) for r in cql.execute(f"SELECT ck, v FROM {table} WHERE pk = {pk} BYPASS CACHE") if r.v is not None]
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 1, 1) USING TIMESTAMP {era + 1}")
        cql.execute(deletions[deletion].format(table=table, pk=pk, ts=era + 10))
        nodetool.flush(cql, table)
        cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, 1, 5) USING TIMESTAMP {era + 5}")
        assert visible() == []
        time.sleep(gc_grace_seconds + 1.1)
        nodetool.compact(cql, table, flush_memtables=False)
        assert visible() == []
        nodetool.flush(cql, table)
        assert visible() == []
        nodetool.compact(cql, table)
        assert visible() == []


# TTL expiry is measured from the write's arrival on the wall clock, whatever
# its timestamp: the cell lives for its TTL - neither expiring at once
# (timestamp in the past) nor living forever (timestamp in the future).
def test_ttl_counts_from_arrival(cql, table_no_gc, era):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 0, 1) USING TIMESTAMP {era + 1} AND TTL 2")
    assert select_rows(cql, table_no_gc, pk) == [(0, 1)]
    ttl = cql.execute(f"SELECT ttl(v) FROM {table_no_gc} WHERE pk = {pk} AND ck = 0").one()[0]
    assert ttl in (1, 2)
    nodetool.flush(cql, table_no_gc)
    assert select_rows(cql, table_no_gc, pk) == [(0, 1)]
    time.sleep(3)
    assert select_rows(cql, table_no_gc, pk) == []
    nodetool.compact(cql, table_no_gc)
    assert select_rows(cql, table_no_gc, pk) == []


# A batch with USING TIMESTAMP applies the client's timestamp to all of its
# statements - also for logged batches, which go through the batchlog.
@pytest.mark.parametrize("logged", [True, False])
def test_batch_using_timestamp(cql, table_no_gc, era, logged):
    pk = unique_key_int()
    batch = "BEGIN BATCH" if logged else "BEGIN UNLOGGED BATCH"
    cql.execute(f"INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 0, 0) USING TIMESTAMP {era + 20}")
    cql.execute(f"""{batch} USING TIMESTAMP {era + 10}
        INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 0, 1)
        INSERT INTO {table_no_gc} (pk, ck, v) VALUES ({pk}, 1, 1)
        APPLY BATCH""")
    assert select_rows(cql, table_no_gc, pk) == [(0, 0), (1, 1)]
    assert list(cql.execute(f"SELECT writetime(v) FROM {table_no_gc} WHERE pk = {pk}")) == [(era + 20,), (era + 10,)]


# Dropping a column hides cells with timestamps up to the drop timestamp.
# Without USING TIMESTAMP the drop timestamp is the wall clock, which is
# meaningless for synthetic timestamps; with it, the drop takes effect in the
# client's timeline - also for a column re-added under the same name. Writes
# to the re-added column must have timestamps above the drop timestamp
# (otherwise they are visible in memtables but filtered from sstables).
def test_drop_column_using_timestamp(cql, test_keyspace, era):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, a int, b int") as table:
        pk = unique_key_int()
        cql.execute(f"INSERT INTO {table} (pk, a, b) VALUES ({pk}, 1, 1) USING TIMESTAMP {era + 5}")
        nodetool.flush(cql, table)
        cql.execute(f"ALTER TABLE {table} DROP b USING TIMESTAMP {era + 10}")
        cql.execute(f"ALTER TABLE {table} ADD b int")
        assert list(cql.execute(f"SELECT a, b FROM {table} WHERE pk = {pk}")) == [(1, None)]
        cql.execute(f"UPDATE {table} USING TIMESTAMP {era + 11} SET b = 11 WHERE pk = {pk}")
        assert list(cql.execute(f"SELECT a, b FROM {table} WHERE pk = {pk}")) == [(1, 11)]
        nodetool.compact(cql, table)
        assert list(cql.execute(f"SELECT a, b FROM {table} WHERE pk = {pk}")) == [(1, 11)]


# Materialized view updates take their timestamps from the base table write,
# so view rows follow the client's timeline.
def test_materialized_view(cql, test_keyspace, era):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        view = table + "_by_v"
        cql.execute(f"CREATE MATERIALIZED VIEW {view} AS SELECT * FROM {table} "
                    f"WHERE pk IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk)")
        try:
            pk = unique_key_int()
            def view_rows():
                return sorted((r.v, r.pk) for r in cql.execute(f"SELECT v, pk FROM {view} WHERE pk = {pk} ALLOW FILTERING"))
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 10) USING TIMESTAMP {era + 10}")
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 5) USING TIMESTAMP {era + 5}")
            assert view_rows() == [(10, pk)]
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 20) USING TIMESTAMP {era + 20}")
            assert view_rows() == [(20, pk)]
            cql.execute(f"DELETE FROM {table} USING TIMESTAMP {era + 15} WHERE pk = {pk}")
            assert view_rows() == [(20, pk)]
            cql.execute(f"DELETE FROM {table} USING TIMESTAMP {era + 30} WHERE pk = {pk}")
            assert view_rows() == []
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 25) USING TIMESTAMP {era + 25}")
            assert view_rows() == []
            nodetool.flush(cql, table)
            nodetool.flush(cql, view)
            nodetool.compact(cql, table)
            nodetool.compact(cql, view)
            assert view_rows() == []
            assert list(cql.execute(f"SELECT * FROM {table} WHERE pk = {pk}")) == []
        finally:
            cql.execute(f"DROP MATERIALIZED VIEW {view}")


# Appending to a non-frozen list derives each element's position (a timeuuid)
# from the write timestamp. A timeuuid holds times between 1582 and ~5236, so
# timestamps outside that range cannot be used for list appends.
def test_list_append(cql, test_keyspace, era):
    if not -12219292800_000_000 <= era <= 100_000_000_000_000_000:
        pytest.skip("SCYLLADB-3885: out-of-range timestamp overflows the timeuuid conversion and triggers on_internal_error")
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, l list<int>") as table:
        pk = unique_key_int()
        cql.execute(f"UPDATE {table} USING TIMESTAMP {era + 1} SET l = l + [1] WHERE pk = {pk}")
        cql.execute(f"UPDATE {table} USING TIMESTAMP {era + 2} SET l = l + [2] WHERE pk = {pk}")
        assert list(cql.execute(f"SELECT l FROM {table} WHERE pk = {pk}")) == [([1, 2],)]
        nodetool.compact(cql, table)
        assert list(cql.execute(f"SELECT l FROM {table} WHERE pk = {pk}")) == [([1, 2],)]
