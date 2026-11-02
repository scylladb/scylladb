# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import json
import os
import shutil
import time
from contextlib import ExitStack

import pytest
import requests
from .util import new_materialized_view, new_test_keyspace, new_test_table, unique_name, sleep_till_whole_second
from . import nodetool, rest_api
from .test_materialized_view import wait_for_view_built
from .test_snapshot import table_dir

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
            wait_for_view_built(cql, mv)
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

# When using tablets, the test test_compactionstats_after_major_compaction
# assumes there is just one tablet - otherwise if there are many, each may
# have so little data that no compation will be required. So the following
# fixture is a keyspace that ensures it either uses vnodes or when using
# tablets - it uses just one.
@pytest.fixture(scope="module")
def test_keyspace_1(cql, this_dc, has_tablets, test_keyspace):
    if has_tablets:
        name = unique_name()
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': true, 'initial': 1 }")
        yield name
    else:
        # The regular test_keyspace is fine, no need to create another one
        yield test_keyspace
    cql.execute("DROP KEYSPACE " + name)

@pytest.mark.parametrize("compaction_strategy", ["LeveledCompactionStrategy", "SizeTieredCompactionStrategy", "TimeWindowCompactionStrategy"])
def test_compactionstats_after_major_compaction(scylla_only, cql, test_keyspace_1, compaction_strategy):
    """
    Test that compactionstats show no pending compaction after major compaction
    """
    num_sstables = 16
    extra_strategy_options = ""
    if compaction_strategy == "LeveledCompactionStrategy":
        extra_strategy_options = ", 'sstable_size_in_mb':1"
        num_sstables *= 4   # Need enough data to trigger level 0 compaction
    value = 'x' * 128*1024
    with new_test_table(cql, test_keyspace_1,
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

# A simple test to exercise nodetool.compact() and nodetool.compact_keyspace()
# for both Scylla and Cassandra. It is less of a test for Scylla, more of a
# test for these utility functions which have different code paths for Scylla
# and Cassandra. We just check the operation doesn't fail - not what it does.
# Unlike Cassandra's nodetool, our nodetool.compact() utility function can
# flush memtables before compacting (and does this by default), so we want
# to check this option enabled and disabled.
def test_nodetool_py_compact(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v int") as table:
        for i in range(2):
            cql.execute(f"INSERT INTO {table} (p, v) VALUES ({i}, {i})")
            nodetool.flush(cql, table)
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (3, 3)")
        nodetool.compact(cql, table, flush_memtables=False)
        nodetool.compact(cql, table, flush_memtables=True)
        nodetool.compact_keyspace(cql, test_keyspace, flush_memtables=False)
        nodetool.compact_keyspace(cql, test_keyspace, flush_memtables=True)

# Test that major compaction - of all tables, or of one table - merges the
# table's two sstables into one.
# Translated from scylla-dtest's nodetool_additional_test.py::TestNodetool
# tests test_general_compact and test_specific_compact.
@pytest.mark.parametrize('scope', ['all', 'table'])
def test_major_compaction_merges_sstables(scylla_only, cql, test_keyspace, scope):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        with nodetool.no_autocompaction_context(cql, table):
            for val in ['a', 'b']:
                cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, '{val}')")
                nodetool.flush(cql, table)
            assert rest_api.get_column_family_metric(cql, 'live_ss_table_count', table) == 2
            if scope == 'all':
                requests.post(f'{nodetool.rest_api_url(cql)}/storage_service/compact').raise_for_status()
            else:
                nodetool.compact(cql, table)
            assert rest_api.get_column_family_metric(cql, 'live_ss_table_count', table) == 1
            assert list(cql.execute(f'SELECT * FROM {table}')) == [(1, 'b')]

#############################################################################
# The following tests were translated from scylla-dtest's compaction_test.py
# (class TestCompaction), which ran each test for each of four compaction
# strategies, so these tests are parametrized by the strategy too. They are
# scylla_only, because they use Scylla's REST API (which nodetool uses) and
# Scylla's mutation_fragments() function to read the sstables.
#############################################################################

compaction_strategies = ['SizeTieredCompactionStrategy', 'LeveledCompactionStrategy',
                         'TimeWindowCompactionStrategy', 'IncrementalCompactionStrategy']

def sstable_count(cql, table):
    return rest_api.get_column_family_metric(cql, 'live_ss_table_count', table)

# Return the number of partition tombstones in the table's sstables
def sstable_partition_tombstones(cql, table):
    return sum(1 for r in cql.execute(f'SELECT * FROM mutation_fragments({table})')
               if r.mutation_source.startswith('sstable:') and r.mutation_fragment_kind == 'partition start'
               and 'deletion_time' in r.metadata)

# Major compaction of the table, which purges every expired tombstone in it.
# Compaction doesn't purge a tombstone while the commitlog may still have
# data written to the table before the tombstone, and the commitlog keeps a
# segment as long as any table has unflushed data in it - so whether a
# test's tombstones would be purged would depend on other tables. So we
# first flush all tables, which also switches to a new commitlog segment and
# frees the old ones. (The dtests ran a global "nodetool compact", which
# did the same the first time it ran.)
def compact_purging_expired_tombstones(cql, table):
    nodetool.flush_all(cql)
    nodetool.compact(cql, table)

# Test that a deletion writes a partition tombstone to the sstable, and
# that compaction keeps the tombstones until gc_grace_seconds pass, and
# then purges them.
# Translated from scylla-dtest's compaction_test.py::TestCompaction tests
# test_compaction_delete and test_compaction_delete_2 (the latter includes
# the former). The dtests waited 25 seconds for gc_grace_seconds to pass;
# this test moves the server's clock instead.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_compaction_purges_expired_tombstones(scylla_only, cql, test_keyspace, clock, strategy):
    gc_grace_seconds = 100
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val int',
                        f"WITH compaction = {{'class': '{strategy}'}} AND gc_grace_seconds = {gc_grace_seconds} "
                        "AND tombstone_gc = {'mode': 'timeout'}") as table:
        for key in range(10):
            cql.execute(f'INSERT INTO {table} (key, val) VALUES ({key}, 1)')
        nodetool.flush(cql, table)
        for key in range(3):
            cql.execute(f'DELETE FROM {table} WHERE key = {key}')
        nodetool.flush(cql, table)
        assert sorted(cql.execute(f'SELECT * FROM {table}')) == [(key, 1) for key in range(3, 10)]
        assert sstable_partition_tombstones(cql, table) == 3
        # Before each compaction, write another sstable, like the dtest did
        # ("so that compact will have something to do").
        # Check that compaction before gc_grace_seconds pass keeps the tombstones
        cql.execute(f'INSERT INTO {table} (key, val) VALUES (99, 1)')
        nodetool.flush(cql, table)
        compact_purging_expired_tombstones(cql, table)
        assert sstable_partition_tombstones(cql, table) == 3
        # Check that compaction after gc_grace_seconds pass purges them
        clock.jump(gc_grace_seconds + 1)
        cql.execute(f'INSERT INTO {table} (key, val) VALUES (99, 1)')
        nodetool.flush(cql, table)
        compact_purging_expired_tombstones(cql, table)
        assert sstable_partition_tombstones(cql, table) == 0
        assert sorted(cql.execute(f'SELECT * FROM {table}')) == [(key, 1) for key in list(range(3, 10)) + [99]]

# Test that when all the data of a table is deleted, and the tombstones can
# be purged immediately (with tombstone_gc mode "immediate"), compaction
# leaves no sstables - neither in the table's list of sstables nor on disk.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_sstable_deletion,
# which slept a second (the resolution of tombstones' deletion time); this
# test moves the server's clock instead.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_compaction_deletes_sstables_of_deleted_data(scylla_only, cql, test_keyspace, clock, scylla_data_dir, strategy):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val int',
                        f"WITH compaction = {{'class': '{strategy}'}} AND tombstone_gc = {{'mode': 'immediate'}}") as table:
        for key in range(10):
            cql.execute(f'INSERT INTO {table} (key, val) VALUES ({key}, {key})')
        nodetool.flush(cql, table)
        for key in range(10):
            cql.execute(f'DELETE FROM {table} WHERE key = {key}')
        clock.jump(1)
        nodetool.flush(cql, table)
        directory = table_dir(cql, scylla_data_dir, table)
        def data_files():
            return [f for f in os.listdir(directory) if f.endswith('-Data.db')]
        assert sstable_count(cql, table) > 0
        assert data_files()
        compact_purging_expired_tombstones(cql, table)
        assert sstable_count(cql, table) == 0
        # Check that the compacted sstables' files were deleted from disk.
        # They may be deleted only after the compaction finishes, so wait.
        deadline = time.time() + 30
        while data_files() and time.time() < deadline:
            time.sleep(0.1)
        assert data_files() == []

# Test that changing a table's compaction strategy - and changing it back -
# doesn't lose data or revive deleted data, that the schema has the new
# strategy, and that major compaction with the new strategy works.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_compaction_strategy_switching,
# which didn't check one of the rows (key 10), and from scylla-dtest's
# ics_compaction_test.py::TestIcsCompaction tests test_alter_table_to_ics,
# test_alter_table_from_ics, test_alter_table_strategy_twice and
# test_lcs_major_compaction_then_ics_major_compaction, which checked only
# the strategy in the schema and that the major compaction succeeded.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_compaction_strategy_switching(scylla_only, cql, test_keyspace, strategy):
    for new_strategy in compaction_strategies:
        if new_strategy == strategy:
            continue
        with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val int',
                            f"WITH gc_grace_seconds = 0 AND compaction = {{'class': '{strategy}'}}") as table:
            ks, cf = table.split('.')
            def check(expected_strategy):
                assert sorted(cql.execute(f'SELECT * FROM {table}')) == [(key, 1) for key in range(3, 10)]
                compaction = cql.execute(f"SELECT compaction FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one().compaction
                assert compaction['class'].endswith(expected_strategy)
            for key in range(10):
                cql.execute(f'INSERT INTO {table} (key, val) VALUES ({key}, 1)')
            nodetool.flush(cql, table)
            for key in range(3):
                cql.execute(f'DELETE FROM {table} WHERE key = {key}')
            for alter_to in [new_strategy, strategy]:
                cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': '{alter_to}'}}")
                check(alter_to)
                nodetool.flush(cql, table)
                check(alter_to)
                nodetool.compact(cql, table)
                check(alter_to)

#############################################################################
# The following tests check disabling and enabling the automatic compaction
# of a table - with the REST API, which "nodetool disableautocompaction" and
# "nodetool enableautocompaction" use, or with the table's "enabled"
# compaction option. A test checks that a table isn't compacted by flushing
# several sstables into it and into a reference table whose autocompaction
# is enabled: When the reference table was compacted, the first table would
# have been compacted too if its autocompaction had been enabled. The dtests
# instead looked for compactions of the tables in system.compaction_history
# for 2 seconds. All the sstables are of one partition, so they are all in
# the same compaction group (shard or tablet).
#############################################################################

flushes = 6

def autocompaction_url(cql, table):
    ks, cf = table.split('.')
    return f'{nodetool.rest_api_url(cql)}/column_family/autocompaction/{ks}:{cf}'

def autocompaction_enabled(cql, table):
    response = requests.get(autocompaction_url(cql, table))
    response.raise_for_status()
    return response.json()

def set_autocompaction(cql, table, enabled):
    response = requests.request('POST' if enabled else 'DELETE', autocompaction_url(cql, table))
    response.raise_for_status()

def flush_sstables(cql, table):
    for ck in range(flushes):
        cql.execute(f"INSERT INTO {table} (key, ck, val) VALUES (0, {ck}, 'hello')")
        nodetool.flush(cql, table)

def wait_for_compaction(cql, table, count, timeout=60):
    deadline = time.time() + timeout
    while sstable_count(cql, table) >= count:
        assert time.time() < deadline, f'{table} was not compacted'
        time.sleep(0.01)

# Flush sstables into the table and the reference table, and check that
# the reference table is compacted but the table isn't. Then compact the
# table (and the reference table) to one sstable, for the next check.
def check_not_compacted(cql, table, reference):
    before = sstable_count(cql, table)
    flush_sstables(cql, table)
    reference_before = sstable_count(cql, reference)
    flush_sstables(cql, reference)
    wait_for_compaction(cql, reference, reference_before + flushes)
    assert sstable_count(cql, table) == before + flushes
    nodetool.compact(cql, table)
    nodetool.compact(cql, reference)

# Flush sstables into the table and check that it is compacted
def check_compacted(cql, table):
    before = sstable_count(cql, table)
    flush_sstables(cql, table)
    wait_for_compaction(cql, table, before + flushes)

def new_compaction_test_table(cql, keyspace, strategy, enabled=None):
    options = f"'class': '{strategy}'" + (f", 'enabled': '{enabled}'" if enabled is not None else '')
    return new_test_table(cql, keyspace, 'key int, ck int, val text, PRIMARY KEY (key, ck)',
                          f'WITH compaction = {{{options}}}')

# Test disabling and enabling autocompaction of a table with the REST API:
# disabling it doesn't affect other tables, and disabling it twice works.
# Translated from scylla-dtest's compaction_test.py::TestCompaction tests
# test_disable_autocompaction_nodetool and test_disable_autocompaction_twice.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_disable_autocompaction(scylla_only, cql, test_keyspace, strategy):
    with new_compaction_test_table(cql, test_keyspace, strategy) as table, \
         new_compaction_test_table(cql, test_keyspace, strategy) as reference:
        set_autocompaction(cql, table, False)
        assert not autocompaction_enabled(cql, table)
        assert autocompaction_enabled(cql, reference)
        check_not_compacted(cql, table, reference)
        set_autocompaction(cql, table, False)
        set_autocompaction(cql, table, False)
        assert not autocompaction_enabled(cql, table)
        check_not_compacted(cql, table, reference)
        set_autocompaction(cql, table, True)
        assert autocompaction_enabled(cql, table)
        assert autocompaction_enabled(cql, reference)
        check_compacted(cql, table)
        check_compacted(cql, reference)

# Test disabling and enabling autocompaction of all tables - for each
# keyspace, with the REST API which "nodetool disableautocompaction" without
# parameters uses. The reference table is in a keyspace created after
# autocompaction was disabled, so its autocompaction is enabled.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_disable_autocompaction_without_params.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_disable_autocompaction_all_tables(scylla_only, cql, test_keyspace, strategy):
    with new_compaction_test_table(cql, test_keyspace, strategy) as table1, \
         new_compaction_test_table(cql, test_keyspace, strategy) as table2:
        url = f'{nodetool.rest_api_url(cql)}/storage_service/auto_compaction'
        keyspaces = [r.keyspace_name for r in cql.execute('SELECT keyspace_name FROM system_schema.keyspaces')]
        try:
            for ks in keyspaces:
                requests.delete(f'{url}/{ks}').raise_for_status()
            assert not autocompaction_enabled(cql, table1)
            assert not autocompaction_enabled(cql, table2)
            with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks, \
                 new_compaction_test_table(cql, ks, strategy) as reference:
                assert autocompaction_enabled(cql, reference)
                flush_sstables(cql, table1)
                flush_sstables(cql, table2)
                flush_sstables(cql, reference)
                wait_for_compaction(cql, reference, flushes)
                assert sstable_count(cql, table1) == flushes
                assert sstable_count(cql, table2) == flushes
        finally:
            # Re-enable autocompaction of all the keyspaces before checking
            # for errors, so a failure doesn't leave it disabled for other
            # tests on the shared server.
            responses = [requests.post(f'{url}/{ks}') for ks in keyspaces]
            for response in responses:
                response.raise_for_status()
        assert autocompaction_enabled(cql, table1)
        assert autocompaction_enabled(cql, table2)
        check_compacted(cql, table1)
        check_compacted(cql, table2)

# Test disabling and enabling autocompaction of a table with its "enabled"
# compaction option, in CREATE TABLE and ALTER TABLE.
# Translated from scylla-dtest's compaction_test.py::TestCompaction tests
# test_disable_autocompaction_schema and test_disable_autocompaction_alter.
# The first also checked that the option still disables autocompaction
# after a restart; this test doesn't restart Scylla.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_disable_autocompaction_with_option(scylla_only, cql, test_keyspace, strategy):
    with new_compaction_test_table(cql, test_keyspace, strategy, 'false') as table1, \
         new_compaction_test_table(cql, test_keyspace, strategy) as table2, \
         new_compaction_test_table(cql, test_keyspace, strategy) as reference:
        cql.execute(f"ALTER TABLE {table2} WITH compaction = {{'class': '{strategy}', 'enabled': 'false'}}")
        for table in [table1, table2]:
            check_not_compacted(cql, table, reference)
            cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': '{strategy}', 'enabled': 'true'}}")
            check_compacted(cql, table)

# Test that changing the compaction options of a table whose autocompaction
# was disabled with the REST API keeps it disabled.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_disable_autocompaction_alter_and_nodetool.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_disable_autocompaction_survives_alter(scylla_only, cql, test_keyspace, strategy):
    with new_compaction_test_table(cql, test_keyspace, strategy) as table, \
         new_compaction_test_table(cql, test_keyspace, strategy) as reference:
        set_autocompaction(cql, table, False)
        check_not_compacted(cql, table, reference)
        cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': '{strategy}', 'tombstone_threshold': 0.9}}")
        assert not autocompaction_enabled(cql, table)
        check_not_compacted(cql, table, reference)
        set_autocompaction(cql, table, True)
        check_compacted(cql, table)

# Test that disabling autocompaction of a table doesn't prevent the user
# from upgrading its sstables (with "nodetool upgradesstables"): The
# upgrade rewrites all the table's sstables, and autocompaction remains
# disabled.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_disable_autocompaction_doesnt_block_user_initiated_upgrade_compaction,
# which checked the log message of the upgrade.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_disable_autocompaction_allows_upgrade(scylla_only, cql, test_keyspace, strategy):
    with new_compaction_test_table(cql, test_keyspace, strategy) as table:
        set_autocompaction(cql, table, False)
        flush_sstables(cql, table)
        def sstables():
            return {r.mutation_source for r in cql.execute(f'SELECT mutation_source FROM mutation_fragments({table})')
                    if r.mutation_source.startswith('sstable:')}
        before = sstables()
        assert len(before) == flushes
        ks, cf = table.split('.')
        requests.get(f'{nodetool.rest_api_url(cql)}/storage_service/keyspace_upgrade_sstables/{ks}', params={'cf': cf}).raise_for_status()
        after = sstables()
        assert len(after) == flushes and not (after & before)
        assert not autocompaction_enabled(cql, table)

# Test that disabling autocompaction of a table doesn't prevent reshaping the
# sstables which "nodetool refresh" loads: The test copies the table's own
# sstables to its upload directory, truncates the table, changes its
# compaction strategy to TimeWindowCompactionStrategy, and loads them. Each
# of these sstables has rows of three one-minute time windows, so TWCS has
# to reshape them - into sstables of a single window each - however few of
# them there are. Without tablets, refresh reshapes the sstables as it loads
# them. With tablets, refresh streams the sstables into the table
# (load-and-stream), and the reshape is done by off-strategy compaction,
# which the test triggers with the REST API instead of waiting for its
# 5-minute timer. Without tablets, that request does nothing.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_disable_autocompaction_doesnt_block_user_initiated_reshape_compaction,
# which checked the log message of the reshape, and ran with and without
# tablets in different CI jobs.
@pytest.mark.parametrize('test_keyspace', ['tablets', 'vnodes'], indirect=True)
def test_disable_autocompaction_allows_reshape(scylla_only, cql, test_keyspace, scylla_data_dir):
    with new_compaction_test_table(cql, test_keyspace, 'SizeTieredCompactionStrategy') as table:
        set_autocompaction(cql, table, False)
        first_window = past_window_start()
        for flush in range(flushes):
            for window in range(3):
                timestamp = (first_window + window * 60) * 1000000
                cql.execute(f"INSERT INTO {table} (key, ck, val) VALUES (0, {flush * 3 + window}, 'hello') USING TIMESTAMP {timestamp}")
            nodetool.flush(cql, table)
        directory = table_dir(cql, scylla_data_dir, table)
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f)):
                shutil.copy(os.path.join(directory, f), os.path.join(directory, 'upload'))
        cql.execute(f'TRUNCATE {table}')
        cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': 'TimeWindowCompactionStrategy', "
                    "'compaction_window_size': 1, 'compaction_window_unit': 'MINUTES', 'max_threshold': 2, 'min_threshold': 2}")
        assert not autocompaction_enabled(cql, table)
        ks, cf = table.split('.')
        requests.post(f'{nodetool.rest_api_url(cql)}/storage_service/sstables/{ks}', params={'cf': cf}).raise_for_status()
        requests.post(f'{nodetool.rest_api_url(cql)}/storage_service/keyspace_offstrategy_compaction/{ks}', params={'cf': cf}).raise_for_status()
        assert sorted(r.ck for r in cql.execute(f'SELECT ck FROM {table}')) == list(range(flushes * 3))
        # Check that the sstables were reshaped: each has rows of one window
        windows = [(low // 60000000, high // 60000000) for low, high in sstable_timestamps(cql, table).values()]
        assert windows and all(low == high for low, high in windows)
        assert {low for low, high in windows} == {first_window // 60 + window for window in range(3)}
        assert not autocompaction_enabled(cql, table)

#############################################################################
# The following tests were translated from scylla-dtest's
# compaction_additional_test.py. The tests of TimeWindowCompactionStrategy
# write all the rows to one partition, so all of a table's sstables are in
# the same compaction group (shard or tablet), and write them with explicit
# timestamps in past one-minute time windows.
#############################################################################

# Return a dict mapping each of the table's sstables to the minimum and
# maximum write timestamps of the rows in it
def sstable_timestamps(cql, table):
    timestamps = {}
    for r in cql.execute(f'SELECT mutation_source, mutation_fragment_kind, metadata FROM mutation_fragments({table})'):
        if r.mutation_source.startswith('sstable:') and r.mutation_fragment_kind == 'clustering row':
            metadata = json.loads(r.metadata)
            row_timestamps = [metadata[k]['timestamp'] for k in ['marker', 'tombstone'] if metadata.get(k)]
            row_timestamps += [c['timestamp'] for c in metadata.get('columns', {}).values()]
            low, high = timestamps.get(r.mutation_source, (min(row_timestamps), max(row_timestamps)))
            timestamps[r.mutation_source] = (min(low, *row_timestamps), max(high, *row_timestamps))
    return timestamps

def wait_for_compactions(cql):
    deadline = time.time() + 60
    while rest_api.get_request(cql, 'compaction_manager/compactions'):
        assert time.time() < deadline, 'compactions did not finish'
        time.sleep(0.01)

# The start, in seconds, of a one-minute time window ten minutes ago
def past_window_start():
    return (int(time.time()) // 60 - 10) * 60

# Test that when a memtable has both rows and the deletions of these rows,
# the sstable which flushing it writes has only the deletions.
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditional::test_compact_tombstones_when_memtable_flush_one_node.
def test_flush_drops_deleted_rows(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'i int PRIMARY KEY, t text') as table:
        for i in range(10):
            cql.execute(f"INSERT INTO {table} (i, t) VALUES ({i}, 'hello')")
        for i in range(3):
            cql.execute(f'DELETE FROM {table} WHERE i = {i}')
        nodetool.flush(cql, table)
        deleted = set()
        rows = set()
        for r in cql.execute(f'SELECT * FROM mutation_fragments({table})'):
            if r.mutation_source.startswith('sstable:'):
                if r.mutation_fragment_kind == 'partition start' and 'deletion_time' in r.metadata:
                    deleted.add(r.i)
                elif r.mutation_fragment_kind == 'clustering row':
                    rows.add(r.i)
        assert deleted == {0, 1, 2}
        assert rows == set(range(3, 10))
        assert sorted(r.i for r in cql.execute(f'SELECT i FROM {table}')) == list(range(3, 10))

# Test that after a major compaction of a table with TimeWindowCompactionStrategy,
# writing data in newer time windows doesn't compact the sstables of the
# older windows again.
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditional::test_compact_data_by_time_window,
# which, like this test, used millisecond timestamps.
def test_twcs_old_windows_not_compacted_again(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v text, PRIMARY KEY (p, c)',
                        "WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1, "
                        "'compaction_window_unit': 'MINUTES', 'timestamp_resolution': 'MILLISECONDS'}") as table:
        first_window_ms = past_window_start() * 1000
        # Each window has its own rows, so a compaction which merges windows
        # doesn't make the older windows' versions of the rows disappear.
        def write_window(window):
            for _ in range(7):
                for c in range(window * 10, window * 10 + 10):
                    cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (0, {c}, 'hello') USING TIMESTAMP {first_window_ms + window * 60000}")
                nodetool.flush(cql, table)
        for window in range(4):
            write_window(window)
        nodetool.compact(cql, table)
        sstables = set(sstable_timestamps(cql, table))
        assert len(sstables) == 4
        for window in range(4, 6):
            write_window(window)
        wait_for_compactions(cql)
        assert sstables <= set(sstable_timestamps(cql, table))

# Test that major compaction of a table with TimeWindowCompactionStrategy
# compacts the sstables of each time window into one sstable, and doesn't
# merge different windows.
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditional::test_major_compaction_with_several_timewindows,
# which wrote many partitions, so it could only check that each window had
# at most one sstable per shard. This test writes one partition, so it
# checks that each window has exactly one sstable.
def test_twcs_major_compaction_per_window(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'p int, c int, v text, PRIMARY KEY (p, c)',
                        "WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1, "
                        "'compaction_window_unit': 'MINUTES', 'min_threshold': 7, 'expired_sstable_check_frequency_seconds': 60}") as table:
        first_window = past_window_start()
        windows = 3
        for window in range(windows):
            # More flushes than min_threshold. Each window has its own rows,
            # which are written again in several flushes in the window.
            for flush in range(8):
                timestamp = (first_window + window * 60 + flush * 5) * 1000000
                for c in range(window * 100 + flush * 5, window * 100 + flush * 5 + 10):
                    cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (0, {c}, 'hello') USING TIMESTAMP {timestamp}")
                nodetool.flush(cql, table)
        def sstable_windows():
            return {sstable: (low // 60000000, high // 60000000) for sstable, (low, high) in sstable_timestamps(cql, table).items()}
        expected_windows = {first_window // 60 + window for window in range(windows)}
        # Check that no sstable has data of more than one window
        assert all(low == high for low, high in sstable_windows().values())
        assert {low for low, high in sstable_windows().values()} == expected_windows
        nodetool.compact(cql, table)
        assert sorted(low for low, high in sstable_windows().values()) == sorted(expected_windows)
        assert all(low == high for low, high in sstable_windows().values())

# Test that compaction of a table with TimeWindowCompactionStrategy removes
# sstables whose data has all expired (by the table's default TTL), but only
# when a compaction is triggered (here, by writing and flushing new data).
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditional::test_compaction_removes_ttld_data_by_time_windows.
# The dtest waited for the 20-second TTL to pass; this test moves the
# server's clock instead.
def test_twcs_removes_expired_sstables(scylla_only, cql, test_keyspace, clock):
    # A TTL long enough that nothing expires before the clock jumps past it
    ttl = clock.ttl
    gc_grace = 1
    with new_test_table(cql, test_keyspace, 'p int, c int, v text, PRIMARY KEY (p, c)',
                        f"WITH default_time_to_live = {ttl} AND gc_grace_seconds = {gc_grace} "
                        "AND tombstone_gc = {'mode': 'immediate'} AND compaction = {'class': 'TimeWindowCompactionStrategy', "
                        "'compaction_window_size': 1, 'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': 1}") as table:
        first_window = past_window_start()
        # 11 flushes, 15 seconds apart, span 3 one-minute time windows
        for flush in range(11):
            timestamp = (first_window + flush * 15) * 1000000
            for c in range(10):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (0, {c}, 'hello') USING TIMESTAMP {timestamp}")
            nodetool.flush(cql, table)
        wait_for_compactions(cql)
        sstables = set(sstable_timestamps(cql, table))
        assert sstables
        clock.jump(ttl + gc_grace + 2)
        # Check that the expired sstables remain until a compaction is triggered
        wait_for_compactions(cql)
        assert set(sstable_timestamps(cql, table)) == sstables
        # Check that the next compaction removes the expired sstables. Like
        # compact_purging_expired_tombstones() above, we first flush all
        # tables, to free the commitlog segments which have the table's data.
        nodetool.flush_all(cql)
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 0, 'hello')")
        nodetool.flush(cql, table)
        wait_for_compactions(cql)
        assert not (set(sstable_timestamps(cql, table)) & sstables)
        # Check that only the new row, which didn't expire, is left
        assert list(cql.execute(f'SELECT * FROM {table}')) == [(1, 0, 'hello')]
        # Check that the expired sstables were dropped, not rewritten (e.g.,
        # into sstables of tombstones): the only sstable left is the new
        # row's. The dtest checked this in the log ("Compacted N sstables
        # to []").
        assert sstable_count(cql, table) == 1

# Test that major compaction compacts the smaller tables first: Each of
# the three tables, of very different sizes, has one partition - with the
# same key, in a keyspace without tablets, so all the tables' data is on the
# same shard. The ids of the compactions in system.compaction_history are
# time UUIDs taken when each compaction starts, so they give the order in
# which the tables were compacted.
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditional::test_major_compaction_processes_tables_in_order_by_size,
# which compared the tables' last compaction times, in milliseconds, and
# sorted ties by the tables' sizes - so it passed if the tables were
# compacted in the same millisecond, whatever their order.
@pytest.mark.parametrize('sizes', [(100, 10000, 100000), (100, 100000, 10000), (10000, 100, 100000),
                                   (10000, 100000, 100), (100000, 100, 10000), (100000, 10000, 100)],
                         ids=lambda sizes: '-'.join(str(size) for size in sizes))
def test_major_compaction_order_by_table_size(scylla_only, cql, has_tablets, sizes):
    tablets = " AND tablets = {'enabled': false}" if has_tablets else ''
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}" + tablets) as ks, \
         ExitStack() as stack:
        tables = [stack.enter_context(new_test_table(cql, ks, 'p int PRIMARY KEY, v blob')) for _ in sizes]
        for table, size in zip(tables, sizes):
            cql.execute(f'INSERT INTO {table} (p, v) VALUES (0, ?)'.replace('?', '0x' + os.urandom(size).hex()))
            nodetool.flush(cql, table)
        nodetool.compact_keyspace(cql, ks)
        history = sorted(cql.execute(f"SELECT id, columnfamily_name, shard_id FROM system.compaction_history WHERE keyspace_name = '{ks}' ALLOW FILTERING"),
                         key=lambda r: r.id.time)
        assert len({r.shard_id for r in history}) == 1
        assert [r.columnfamily_name for r in history] == [table.split('.')[1] for size, table in sorted(zip(sizes, tables))]

# Test that although compaction of a table with TimeWindowCompactionStrategy
# normally doesn't merge sstables of different time windows, major
# compaction merges them with a newer sstable which deletes their data.
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditional::test_data_from_different_windows_compacted_together.
def test_twcs_compacts_deleted_data_of_different_windows(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'pk int, ck int, v int, PRIMARY KEY (pk, ck)',
                        "WITH compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'MINUTES', "
                        "'compaction_window_size': 1} AND tombstone_gc = {'mode': 'disabled'}") as table:
        # Each row is in a different time window
        for ck, timestamp in [(2, 100000000001), (3, 200000000001), (4, 300000000001), (5, 500000000001)]:
            cql.execute(f'INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, 1) USING TIMESTAMP {timestamp}')
        nodetool.flush(cql, table)
        nodetool.compact(cql, table)
        assert sstable_count(cql, table) == 4
        cql.execute(f'DELETE FROM {table} WHERE pk = 1')
        nodetool.flush(cql, table)
        nodetool.compact(cql, table)
        assert sstable_count(cql, table) == 1
        assert list(cql.execute(f'SELECT * FROM {table}')) == []

# Test that compaction removes data whose TTL expired.
# Translated from scylla-dtest's compaction_additional_test.py::TestCompactionAdditionalStrategy::test_compaction_removes_ttld_data_after_gc_period,
# which waited 6 seconds for the 3-second TTL to pass, and didn't check
# that the data was in the sstables before it expired (it may have
# expired before it was flushed). This test moves the server's clock.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_compaction_removes_expired_data(scylla_only, cql, test_keyspace, clock, strategy):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val int',
                        f"WITH compaction = {{'class': '{strategy}'}} AND tombstone_gc = {{'mode': 'immediate'}}") as table:
        def sstable_keys():
            return {r.key for r in cql.execute(f'SELECT key, mutation_source, mutation_fragment_kind FROM mutation_fragments({table})')
                    if r.mutation_source.startswith('sstable:') and r.mutation_fragment_kind == 'partition start'}
        for key in range(10):
            cql.execute(f'INSERT INTO {table} (key, val) VALUES ({key}, 1) USING TTL {clock.ttl}')
        nodetool.flush(cql, table)
        assert sstable_keys() == set(range(10))
        clock.jump(clock.ttl + 1)
        cql.execute(f'INSERT INTO {table} (key, val) VALUES (99, 1)')
        compact_purging_expired_tombstones(cql, table)
        assert sstable_keys() == {99}

# The following tests were translated from scylla-dtest's
# compaction_additional_test.py::TestTimeWindowDataSegregation. They write a
# time series of one row per second, for several minutes, with
# TimeWindowCompactionStrategy with one-minute windows. Like the dtests,
# they use small timestamps (seconds since 1970), and flush every 10 rows.

def new_time_series_table(cql, keyspace, gc_grace_seconds=1800):
    return new_test_table(cql, keyspace, 'pk int, ck int, v blob, PRIMARY KEY (pk, ck)',
                          f"WITH default_time_to_live = 1800 AND gc_grace_seconds = {gc_grace_seconds} "
                          "AND tombstone_gc = {'mode': 'timeout'} AND compaction = {'class': 'TimeWindowCompactionStrategy', "
                          "'compaction_window_size': 1, 'compaction_window_unit': 'MINUTES'}")

def write_time_series(cql, table, minutes):
    insert = cql.prepare(f'INSERT INTO {table} (pk, ck, v) VALUES (1, ?, 0x61) USING TIMESTAMP ?')
    for second in range(minutes * 60):
        cql.execute(insert, [second, second * 1000000])
        if second % 10 == 0:
            nodetool.flush(cql, table)

# Delete rows, given as pairs of the row's second (clustering key) and the
# deletion's timestamp in seconds, and flush the deletions.
def delete_time_series_rows(cql, table, deletions):
    delete = cql.prepare(f'DELETE FROM {table} USING TIMESTAMP ? WHERE pk = 1 AND ck = ?')
    for second, timestamp_second in deletions:
        cql.execute(delete, [timestamp_second * 1000000, second])
    nodetool.flush(cql, table)

# Check that each of the table's sstables has data of only one time window
def check_sstable_windows(cql, table):
    for low, high in sstable_timestamps(cql, table).values():
        assert low // 60000000 == high // 60000000

def time_series_cks(cql, table):
    return [r.ck for r in cql.execute(f'SELECT ck FROM {table}')]

# Test that after all the rows of the old time windows are deleted, major
# compaction leaves only the sstable of the deletions.
# Translated from scylla-dtest's compaction_additional_test.py::TestTimeWindowDataSegregation::test_compact_several_timewindows_after_delete_all_rows_in_old_timewindows.
def test_twcs_compaction_after_deleting_all_rows(scylla_only, cql, test_keyspace):
    with new_time_series_table(cql, test_keyspace) as table:
        write_time_series(cql, table, 5)
        nodetool.compact(cql, table)
        check_sstable_windows(cql, table)
        assert sstable_count(cql, table) == 5
        # Delete each minute's rows with timestamps in the sixth minute
        delete_time_series_rows(cql, table, [(ck, 300 + minute * 12) for minute in range(5) for ck in range(minute * 60, minute * 60 + 60)])
        assert sstable_count(cql, table) == 6
        nodetool.compact(cql, table)
        check_sstable_windows(cql, table)
        assert sstable_count(cql, table) == 1
        assert time_series_cks(cql, table) == []

# Test that after some of the rows of each old time window are deleted,
# major compaction keeps the sstables of the old windows (without the
# deleted rows), and the sstable of the deletions.
# Translated from scylla-dtest's compaction_additional_test.py::TestTimeWindowDataSegregation::test_compact_several_timewindows_after_delete_several_rows_per_timewindow,
# which wrote 4 partitions instead of one.
def test_twcs_compaction_after_deleting_some_rows(scylla_only, cql, test_keyspace):
    with new_time_series_table(cql, test_keyspace) as table:
        write_time_series(cql, table, 5)
        nodetool.compact(cql, table)
        assert sstable_count(cql, table) == 5
        deleted = [ck for minute in range(5) for ck in range(minute * 60, minute * 60 + 20)]
        delete_time_series_rows(cql, table, [(ck, 300) for ck in deleted])
        assert sstable_count(cql, table) == 6
        nodetool.compact(cql, table)
        check_sstable_windows(cql, table)
        assert sstable_count(cql, table) == 6
        assert time_series_cks(cql, table) == sorted(set(range(300)) - set(deleted))

# Test that after all the rows of the first time window are deleted, major
# compaction removes that window's sstable, and keeps the other windows'
# sstables and the sstable of the deletions.
# Translated from scylla-dtest's compaction_additional_test.py::TestTimeWindowDataSegregation::test_compact_several_timewindows_after_delete_rows_in_first_timewindow,
# which wrote 5 partitions instead of one.
def test_twcs_compaction_after_deleting_first_window(scylla_only, cql, test_keyspace):
    with new_time_series_table(cql, test_keyspace) as table:
        write_time_series(cql, table, 5)
        nodetool.compact(cql, table)
        assert sstable_count(cql, table) == 5
        delete_time_series_rows(cql, table, [(ck, 300) for ck in range(60)])
        assert sstable_count(cql, table) == 6
        nodetool.compact(cql, table)
        check_sstable_windows(cql, table)
        assert sstable_count(cql, table) == 5
        assert time_series_cks(cql, table) == list(range(60, 300))

# Test that major compaction drops rows of an old time window which a newer
# sstable deletes, keeps the row tombstones until gc_grace_seconds pass, and
# then purges them.
# Translated from scylla-dtest's compaction_additional_test.py::TestTimeWindowDataSegregation::test_compaction_remove_deleted_rows_in_previous_time_window.
# The dtest checked the sstables for clustering rows of the deleted keys,
# which didn't distinguish between live rows and row tombstones, and waited
# 15 seconds for gc_grace_seconds to pass; this test moves the server's
# clock instead.
def test_twcs_compaction_of_deleted_rows_in_old_window(scylla_only, cql, test_keyspace, clock):
    gc_grace_seconds = 15
    with new_time_series_table(cql, test_keyspace, gc_grace_seconds) as table:
        def sstable_rows():
            live, deleted = set(), set()
            for r in cql.execute(f'SELECT ck, mutation_source, mutation_fragment_kind, metadata FROM mutation_fragments({table})'):
                if r.mutation_source.startswith('sstable:') and r.mutation_fragment_kind == 'clustering row':
                    (deleted if json.loads(r.metadata).get('tombstone') else live).add(r.ck)
            return live, deleted
        write_time_series(cql, table, 1)
        nodetool.compact(cql, table)
        assert sstable_count(cql, table) == 1
        delete_time_series_rows(cql, table, [(ck, 60) for ck in range(20)])
        assert sstable_count(cql, table) == 2
        assert sstable_rows() == (set(range(60)), set(range(20)))
        # Check that compaction drops the deleted rows, but keeps the
        # tombstones before gc_grace_seconds pass
        nodetool.compact(cql, table)
        check_sstable_windows(cql, table)
        assert sstable_count(cql, table) == 2
        assert sstable_rows() == (set(range(20, 60)), set(range(20)))
        assert time_series_cks(cql, table) == list(range(20, 60))
        # Check that compaction after gc_grace_seconds pass purges the tombstones
        clock.jump(gc_grace_seconds + 1)
        compact_purging_expired_tombstones(cql, table)
        assert sstable_count(cql, table) == 1
        assert sstable_rows() == (set(range(20, 60)), set())

# Test that validation (scrub in VALIDATE mode) of a table's valid sstables
# reports that they are valid, and doesn't change them.
# Translated from scylla-dtest's compaction_additional_test.py::TestValidationCompaction::test_validation_compaction_with_valid_sstable,
# which checked the log message that the sstables are valid.
def test_scrub_validate_valid_sstables(scylla_only, cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        for key in range(100):
            cql.execute(f"INSERT INTO {table} (key, val) VALUES ({key}, 'hello')")
        nodetool.flush(cql, table)
        before = set(sstable_timestamps(cql, table))
        assert before
        ks, cf = table.split('.')
        # Scrub takes a "pre-scrub" snapshot unless asked not to, and the
        # snapshot would keep the table's directory after the table is dropped
        response = requests.get(f'{nodetool.rest_api_url(cql)}/storage_service/keyspace_scrub/{ks}',
                                params={'cf': cf, 'scrub_mode': 'VALIDATE', 'disable_snapshot': 'true'})
        response.raise_for_status()
        # 0 means that the sstables are valid (3 means validation errors)
        assert response.json() == 0
        assert set(sstable_timestamps(cql, table)) == before
