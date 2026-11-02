# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import glob
import os
import time
from contextlib import ExitStack, contextmanager

from . import nodetool, rest_api
from .util import new_test_keyspace, new_test_table, unique_name



def has_snapshot(snapshots, tag):
    for s in snapshots:
        if s['key'] == tag:
            return s['value']
    return None

def test_snapshot_ttl(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, 'k int PRIMARY KEY') as table:
        write = cql.prepare(f"INSERT INTO {table} (k) VALUES (?)")
        for i in range(10):
            cql.execute(write, [i])
        nodetool.flush(cql, table)

        tag = "test_snapshot"
        ttl = 60

        # Take a snapshot of the table
        created_at = time.time()
        nodetool.take_snapshot(cql, table, tag, ttl=f"{ttl}s")
        expires_at = time.time() + ttl + 1

        # Verify that the snapshot exists
        snapshots = nodetool.list_snapshots(cql)
        print(f"Snapshots for {table}: {snapshots}")
        infos = has_snapshot(snapshots, tag)
        assert len(infos) == 1, f"Expected to find {tag} in {snapshots=}, found {len(infos)} infos"
        # Currently, Cassandra returns the snapshot creation and expiration times in nodetool listsnapshots.
        # Scylla does not support that yet
        info = infos[0]
        if info.get("created_at"):
            created_at = float(info["created_at"])
            assert info.get("expires_at")
            expires_at = float(info["expires_at"])
            assert expires_at - created_at == ttl

#############################################################################
# The following tests were translated from scylla-dtest's
# listsnapshots_test.py::TestNodetoolListSnapshots. They check the sizes of
# snapshots which Scylla reports (and "nodetool listsnapshots" prints)
# against the snapshot's files on disk. The dtests checked only the
# snapshots which were listed, so they would have passed if a snapshot was
# missing from the list; these tests also check that the list has exactly
# the expected tables. They are scylla_only because they take snapshots of
# whole keyspaces with Scylla's REST API, and read Scylla's data directory.
#############################################################################

def snapshot_keyspace(cql, ks, tag):
    rest_api.post_request(cql, f'storage_service/snapshots?tag={tag}&kn={ks}')

# Check that the snapshot with the given tag lists exactly the given tables
# (if tables is not None), and that each one's reported size ("total") is at
# least the size of its files (except the manifest and schema files), and
# at most the disk space allocated for them.
def check_snapshot_sizes(cql, data_dir, tag, tables=None):
    entries = has_snapshot(nodetool.list_snapshots(cql), tag)
    assert entries
    if tables is not None:
        assert sorted(f"{e['ks']}.{e['cf']}" for e in entries) == sorted(tables)
    for e in entries:
        [snapshot_dir] = glob.glob(os.path.join(data_dir, e['ks'], f"{e['cf']}-*", 'snapshots', tag))
        files = [os.path.join(snapshot_dir, f) for f in os.listdir(snapshot_dir) if f not in ['manifest.json', 'schema.cql']]
        size = sum(os.lstat(f).st_size for f in files)
        allocated = sum(os.lstat(f).st_blocks * 512 for f in files)
        assert size <= e['total'] <= allocated, f'{e}: files have {size} bytes, allocated {allocated}'

@contextmanager
def keyspaces_with_tables(cql, nkeyspaces, ntables):
    with ExitStack() as stack:
        tables = {}
        for _ in range(nkeyspaces):
            ks = stack.enter_context(new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"))
            tables[ks] = [stack.enter_context(new_test_table(cql, ks, 'key int PRIMARY KEY, val text')) for _ in range(ntables)]
            for table in tables[ks]:
                cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, 'asdf')")
        yield tables

# Translated from scylla-dtest's listsnapshots_test.py::TestNodetoolListSnapshots::test_one_ks_one_snapshot.
def test_listsnapshots_one_table(cql, test_keyspace, scylla_data_dir, scylla_only):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, 'asdf')")
        tag = unique_name()
        nodetool.take_snapshot(cql, table, tag)
        try:
            check_snapshot_sizes(cql, scylla_data_dir, tag, [table])
        finally:
            nodetool.del_snapshot(cql, tag)

# Translated from scylla-dtest's listsnapshots_test.py::TestNodetoolListSnapshots::test_snapshots_of_system_ks.
def test_listsnapshots_system_keyspace(cql, scylla_data_dir, scylla_only):
    tag = unique_name()
    snapshot_keyspace(cql, 'system', tag)
    try:
        entries = has_snapshot(nodetool.list_snapshots(cql), tag)
        assert entries and all(e['ks'] == 'system' for e in entries)
        check_snapshot_sizes(cql, scylla_data_dir, tag)
    finally:
        nodetool.del_snapshot(cql, tag)

# Test snapshots of several keyspaces, each with several tables, and then,
# after more rows are written to each table, a second snapshot of each
# keyspace.
# Translated from scylla-dtest's listsnapshots_test.py::TestNodetoolListSnapshots
# tests test_snapshot_for_several_kses (the first snapshots) and
# test_several_snapshots_for_several_kses. The dtests used 3 keyspaces of 3
# tables each, and wrote 10,000 rows into each table between the
# snapshots; this test uses 2 keyspaces of 2 tables, and writes one more
# row into each table, which is enough to give the second snapshots
# different files.
def test_listsnapshots_several_keyspaces(cql, scylla_data_dir, scylla_only):
    with keyspaces_with_tables(cql, 2, 2) as tables:
        tags = []
        try:
            for ks in tables:
                tag = unique_name()
                snapshot_keyspace(cql, ks, tag)
                tags.append((tag, ks))
            for tag, ks in tags:
                check_snapshot_sizes(cql, scylla_data_dir, tag, tables[ks])
            # Write another row to each table, so the second snapshot of each
            # keyspace has a new sstable in addition to the first one's
            for ks_tables in tables.values():
                for table in ks_tables:
                    cql.execute(f"INSERT INTO {table} (key, val) VALUES (2, 'asdf')")
            for ks in tables:
                tag = unique_name()
                snapshot_keyspace(cql, ks, tag)
                tags.append((tag, ks))
            for tag, ks in tags:
                check_snapshot_sizes(cql, scylla_data_dir, tag, tables[ks])
            # Each table's second snapshot has more sstables than its first
            def data_files(tag, table):
                ks, cf = table.split('.')
                return glob.glob(os.path.join(scylla_data_dir, ks, f'{cf}-*', 'snapshots', tag, '*-Data.db'))
            for (first_tag, ks), (second_tag, _) in zip(tags[:len(tables)], tags[len(tables):]):
                for table in tables[ks]:
                    assert len(data_files(second_tag, table)) > len(data_files(first_tag, table))
        finally:
            for tag, _ in tags:
                nodetool.del_snapshot(cql, tag)
