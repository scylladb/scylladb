# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
import requests
from .util import new_materialized_view, new_test_table
from . import nodetool
import time

# sleep to let a ttl (of `seconds`) expire and
# the commitlog minimum gc time, in seconds,
# to be greater than the tombstone deletion time
def sleep_till_whole_second(seconds=1):
    t = time.time()
    time.sleep(seconds - (t - int(t)))

def test_tombstone_gc_with_conflict_in_memtable(scylla_only, cql, test_keyspace):
    """
    Regression test for fixed https://github.com/scylladb/scylladb/issues/20423
    """
    schema = "k int, v int, primary key (k, v)"
    with new_test_table(cql, test_keyspace, schema, extra="with gc_grace_seconds = 0") as table:
        with nodetool.no_autocompaction_context(cql, table):
            # Insert initial data into the base table:
            # Row 1 is expected to be garbage collected after expiration
            # This test case tests the ttl case, while `test_tombstone_gc_with_delete_in_memtable`
            # test also explicit deletion.
            cql.execute(f"insert into {table} (k, v) values (1, 1) using timestamp 1 and ttl 1")
            cql.execute(f"insert into {table} (k, v) values (1, 2) using timestamp 2")
            nodetool.flush(cql, table)
            # Delete row 2, this deletion is expected to be kept
            # when we insert backdated live data into the memtable
            cql.execute(f"delete from {table} using timestamp 3 where k=1 and v=2")
            # Flush all tables explicitly now, since this is skipped in the test on purpose by next major compaction.
            nodetool.flush_all(cql)

            sleep_till_whole_second()
            # Re-insert backdated data into the memtable.  It should inhibit tombstone_gc
            cql.execute(f"insert into {table} (k, v) values (1, 3) using timestamp 2")
            # do not flush before major compaction
            nodetool.compact(cql, table, flush_memtables=False)

            res = cql.execute(f"select * from mutation_fragments({table})")
            sstables = set()
            keys = set()
            rows = set()
            for r in res:
                if "sstable" in r.mutation_source:
                    if r.mutation_fragment_kind == "partition start":
                        sstables.add(r.mutation_source)
                        keys.add(r.k)
                    elif r.mutation_fragment_kind == "clustering row":
                        rows.add(r.v)
                        if r.v == 2:
                            assert "tombstone" in r.metadata

            assert len(sstables) == 1, f"Expected single sstable but saw {len(sstables)}: res={list(res)}"
            assert keys == {1}, f"Expected keys=={1} but got {keys}: res={list(res)}"
            assert rows == {2}, f"Expected rows=={2} but got {rows}: res={list(res)}"

def test_tombstone_gc_with_delete_in_memtable(scylla_only, cql, test_keyspace):
    """
    Reproduce https://github.com/scylladb/scylladb/issues/20423
    """
    schema = "k int, v int, primary key (k, v)"
    with new_test_table(cql, test_keyspace, schema, extra="with gc_grace_seconds = 0") as table:
        with nodetool.no_autocompaction_context(cql, table):
            # Insert initial data into the base table
            # This test case tests the explicit deletion case, while `test_tombstone_gc_with_conflict_in_memtable
            # tests the ttl expiration case
            cql.execute(f"insert into {table} (k, v) values (1, 1) using timestamp 1 and ttl 1")
            cql.execute(f"insert into {table} (k, v) values (1, 2) using timestamp 2")
            cql.execute(f"insert into {table} (k, v) values (1, 3) using timestamp 3")
            nodetool.flush(cql, table)
            cql.execute(f"delete from {table} using timestamp 4 where k=1 and v=2")
            # Flush all tables explicitly now, since this is skipped in the test on purpose by next major compaction.
            nodetool.flush_all(cql)

            sleep_till_whole_second()
            # Insert backdated delete into the memtable.  It should not inhibit tombstone_gc
            cql.execute(f"delete from {table} using timestamp 2 where k=1 and v=3")
            # The following insert should not inhibit tombstone_gc since its timestamp is greater than the tombstones
            cql.execute(f"insert into {table} (k, v) values (1, 4) using timestamp 5")
            # do not flush before major compaction
            nodetool.compact(cql, table, flush_memtables=False)

            res = cql.execute(f"select * from mutation_fragments({table})")
            sstables = set()
            keys = set()
            rows = set()
            for r in res:
                if "sstable" in r.mutation_source:
                    print(r)
                    if r.mutation_fragment_kind == "partition start":
                        sstables.add(r.mutation_source)
                        keys.add(r.k)
                    elif r.mutation_fragment_kind == "clustering row":
                        rows.add(r.v)
                        assert not "tombstone" in r.metadata

            assert len(sstables) == 1, f"Expected single sstable but saw {len(sstables)}: res={list(res)}"
            assert keys == {1}, f"Expected keys=={1} but got {keys}: res={list(res)}"
            assert rows == {3}, f"Expected rows=={3} but got {rows}: res={list(res)}"

def test_tombstone_gc_with_materialized_view_update_in_memtable(scylla_only, cql, test_keyspace):
    """
    Reproduce https://github.com/scylladb/scylladb/issues/20424
    """
    schema = "k int primary key, v int, w int"
    with new_test_table(cql, test_keyspace, schema) as table:
        # Create a materialized view with same partition key as the base, and using a regular column in the base as a clustering key in the view
        with new_materialized_view(cql, table, '*', 'k, v', 'k is not null and v is not null', extra="with gc_grace_seconds = 0") as mv:
            with nodetool.no_autocompaction_context(cql, mv):
                # Insert initial data into the base table
                cql.execute(f"insert into {table} (k, v, w) values (1, 1, 1)")
                # Flush the memtable so the following update won't get compacted in the memtable
                nodetool.flush_keyspace(cql, test_keyspace)
                # Update the regular column in the base table, causing a view update
                # with a shadowable row tombstone for the old value and recent row_marker for the new value
                cql.execute(f"insert into {table} (k, v) values (1, 2)")
                # Flush all tables explicitly now, since this is skipped in the test on purpose by next major compaction.
                nodetool.flush_all(cql)

                sleep_till_whole_second()
                # Insert new view update into the memtable by updating the regular column in the base table.
                # It will generate a view update with a shadowable row tombstone for the previous value
                # and the value of a new row with the old value of `w` (with timestamp 1) - that inhibits the purging
                # of the shadowable tombstone in the sstable without the fix for #20424
                cql.execute(f"insert into {table} (k, v) values (1, 3)")
                # do not flush before major compaction
                nodetool.compact(cql, mv, flush_memtables=False)

                res = cql.execute(f"select * from mutation_fragments({mv})")
                sstables = set()
                keys = set()
                rows = set()
                for r in res:
                    if "sstable" in r.mutation_source:
                        if r.mutation_fragment_kind == "partition start":
                            sstables.add(r.mutation_source)
                            keys.add(r.k)
                        elif r.mutation_fragment_kind == "clustering row":
                            rows.add(r.v)
                            assert "shadowable_tombstone" not in r.metadata

                assert len(sstables) == 1, f"Expected single sstable but saw {len(sstables)}: res={list(res)}"
                assert keys == {1}, f"Expected keys=={1} but got {keys}: res={list(res)}"
                assert rows == {2}, f"Expected rows=={2} but got {keys}: res={list(res)}"

def get_compaction_stats(cql, table):
    ks, cf = table.split('.')
    res = requests.get(f'{nodetool.rest_api_url(cql)}/compaction_manager/metrics/pending_tasks_by_table')
    res.raise_for_status()
    stats = res.json()
    tasks = 0
    for s in stats:
        if s['ks'] == ks and s['cf'] == cf:
            tasks += int(s['task'])
    return tasks, stats

@pytest.mark.parametrize("compaction_strategy", ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy"])
def test_compactionstats_after_major_compaction(scylla_only, cql, test_keyspace, compaction_strategy):
    """
    Test that compactionstats show no pending compaction after major compaction
    """
    num_sstables = 16
    extra_strategy_options = ""
    if compaction_strategy == "LeveledCompactionStrategy":
        extra_strategy_options = ", 'sstable_size_in_mb':1"
        num_sstables *= 4   # Need enough data to trigger level 0 compaction
    value = 'x' * 128*1024
    with new_test_table(cql, test_keyspace,
                        schema="p int PRIMARY KEY, v text",
                        extra=f"WITH compression={{}} AND compaction={{'class':'{compaction_strategy}'{extra_strategy_options}}}") as table:
        with nodetool.no_autocompaction_context(cql, table):
            for i in range(num_sstables):
                cql.execute(f"INSERT INTO {table} (p, v) VALUES ({i}, '{value}')")
                nodetool.flush(cql, table)
            tasks, stats = get_compaction_stats(cql, table)
            assert tasks > 0, f"Found no pending compaction tasks as expected: stats={stats}"
            nodetool.compact(cql, table)
            tasks, stats = get_compaction_stats(cql, table)
            assert tasks == 0, f"Found {tasks} pending compaction tasks unexpectedly: stats={stats}"
