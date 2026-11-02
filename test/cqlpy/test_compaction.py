# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import os
import shutil
import time

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
# leaves no sstables.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_sstable_deletion,
# which slept a second (the resolution of tombstones' deletion time); this
# test moves the server's clock instead.
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_compaction_deletes_sstables_of_deleted_data(scylla_only, cql, test_keyspace, clock, strategy):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val int',
                        f"WITH compaction = {{'class': '{strategy}'}} AND tombstone_gc = {{'mode': 'immediate'}}") as table:
        for key in range(10):
            cql.execute(f'INSERT INTO {table} (key, val) VALUES ({key}, {key})')
        nodetool.flush(cql, table)
        for key in range(10):
            cql.execute(f'DELETE FROM {table} WHERE key = {key}')
        clock.jump(1)
        nodetool.flush(cql, table)
        assert sstable_count(cql, table) > 0
        compact_purging_expired_tombstones(cql, table)
        assert sstable_count(cql, table) == 0

# Test that changing a table's compaction strategy doesn't lose data or
# revive deleted data.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_compaction_strategy_switching,
# which didn't check one of the rows (key 10).
@pytest.mark.parametrize('strategy', compaction_strategies)
def test_compaction_strategy_switching(scylla_only, cql, test_keyspace, strategy):
    for new_strategy in compaction_strategies:
        if new_strategy == strategy:
            continue
        with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val int',
                            f"WITH gc_grace_seconds = 0 AND compaction = {{'class': '{strategy}'}}") as table:
            for key in range(10):
                cql.execute(f'INSERT INTO {table} (key, val) VALUES ({key}, 1)')
            nodetool.flush(cql, table)
            for key in range(3):
                cql.execute(f'DELETE FROM {table} WHERE key = {key}')
            cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': '{new_strategy}'}}")
            assert sorted(cql.execute(f'SELECT * FROM {table}')) == [(key, 1) for key in range(3, 10)]
            nodetool.flush(cql, table)
            assert sorted(cql.execute(f'SELECT * FROM {table}')) == [(key, 1) for key in range(3, 10)]

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
# sstables which "nodetool refresh" loads: TimeWindowCompactionStrategy with
# max_threshold 2 needs to reshape many sstables of the same time window.
# The test copies the table's own sstables to its upload directory, and
# truncates it, to load them. It uses a keyspace without tablets, because
# with tablets the sstables are streamed into the table, not reshaped.
# Translated from scylla-dtest's compaction_test.py::TestCompaction::test_disable_autocompaction_doesnt_block_user_initiated_reshape_compaction,
# which checked the log message of the reshape.
def test_disable_autocompaction_allows_reshape(scylla_only, cql, test_keyspace_vnodes, scylla_data_dir):
    with new_compaction_test_table(cql, test_keyspace_vnodes, 'SizeTieredCompactionStrategy') as table:
        set_autocompaction(cql, table, False)
        flush_sstables(cql, table)
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
        assert sorted(r.ck for r in cql.execute(f'SELECT ck FROM {table}')) == list(range(flushes))
        assert sstable_count(cql, table) < flushes
        assert not autocompaction_enabled(cql, table)
