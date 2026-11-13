# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for loading sstables which were copied into a table's "upload"
# directory, with the REST API request which "nodetool refresh" uses. These
# tests are scylla_only, because they use Scylla's REST API and write files
# in Scylla's data directory.
#############################################################################

import os
import re
import shutil
from contextlib import nullcontext

import pytest
import requests

from test.pylib.skip_types import skip_env

from . import nodetool
from .test_snapshot import restore_with_refresh, save_snapshot, snapshot_tables, table_dir
from .util import new_test_table, unique_name

# Test that loading sstables from the upload directory fails, with an error
# message which explains why, when the directory isn't writable, when the
# sstables in it aren't readable, and when it has a symbolic link.
# Translated from scylla-dtest's nodetool_additional_test.py::TestNodetool::test_nodetool_refresh_with_wrong_upload_modes.
def test_refresh_bad_upload_directory(cql, test_keyspace, scylla_data_dir, scylla_only):
    if os.geteuid() == 0:
        skip_env('File permissions do not apply to root')
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, 'asdf')")
        nodetool.flush(cql, table)
        # Copy the table's sstables into its upload directory
        directory = table_dir(cql, scylla_data_dir, table)
        upload_dir = os.path.join(directory, 'upload')
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f)):
                shutil.copy(os.path.join(directory, f), upload_dir)
        ks, cf = table.split('.')
        def refresh_error():
            response = requests.post(f'{nodetool.rest_api_url(cql)}/storage_service/sstables/{ks}', params={'cf': cf})
            assert not response.ok
            return response.json()['message']
        # Check loading from an upload directory which isn't writable
        os.chmod(upload_dir, 0o555)
        try:
            assert re.search(r'Directory cannot be accessed .* write', refresh_error())
        finally:
            os.chmod(upload_dir, 0o755)
        # Check loading sstables which aren't readable
        files = [os.path.join(upload_dir, f) for f in os.listdir(upload_dir)]
        for f in files:
            os.chmod(f, 0o044)
        try:
            assert re.search(r'File cannot be accessed for read|open failed: Permission denied', refresh_error())
        finally:
            for f in files:
                os.chmod(f, 0o644)
        # Check loading from an upload directory which has a symbolic link
        symlink = os.path.join(upload_dir, 'test_symlink')
        os.symlink('broken', symlink)
        try:
            assert 'Must be either a regular file or a directory' in refresh_error()
        finally:
            os.remove(symlink)

# Write rows 0-3 to a table with the given compaction strategy, each in its
# own sstable, take a snapshot, write rows 4-7, change the table's compaction
# strategy to IncrementalCompactionStrategy, and load the snapshot's sstables
# back into the table: The table should have both the snapshot's rows and
# the newer rows. If truncate is true, the table is truncated after the
# snapshot, so the snapshot's rows can only come back by loading its
# sstables. If it is false, the table's autocompaction is disabled, so the
# table still has the snapshot's sstables - with the same names - when they
# are loaded back.
def check_refresh_snapshot_into_ics_table(cql, keyspace, scylla_data_dir, temp_workdir, strategy, truncate):
    with new_test_table(cql, keyspace, 'key int PRIMARY KEY, val text', f"WITH compaction = {{'class': '{strategy}'}}") as table, \
         (nullcontext() if truncate else nodetool.no_autocompaction_context(cql, table)):
        def write(keys):
            for key in keys:
                cql.execute(f"INSERT INTO {table} (key, val) VALUES ({key}, 'value{key}')")
                nodetool.flush(cql, table)
        write(range(4))
        tag = unique_name()
        snapshot_tables(cql, [table], tag)
        try:
            saved = save_snapshot(cql, scylla_data_dir, table, tag, temp_workdir)
        finally:
            nodetool.del_snapshot(cql, tag)
        if truncate:
            cql.execute(f'TRUNCATE {table}')
        write(range(4, 8))
        cql.execute(f"ALTER TABLE {table} WITH compaction = {{'class': 'IncrementalCompactionStrategy'}}")
        if not truncate:
            # Check that the snapshot's sstables have the same names as some
            # of the table's sstables
            def data_files(directory):
                return {f for f in os.listdir(directory) if f.endswith('-Data.db')}
            assert data_files(saved) and data_files(saved) <= data_files(table_dir(cql, scylla_data_dir, table))
        restore_with_refresh(cql, scylla_data_dir, saved, table)
        assert sorted(cql.execute(f'SELECT * FROM {table}')) == [(key, f'value{key}') for key in range(8)]

# Test loading the sstables of a snapshot back into the same table, after
# it was truncated, more data was written to it and its compaction strategy
# was changed to IncrementalCompactionStrategy. The tests of refresh into an
# ICS table run both with and without tablets, as the dtests did (in
# different CI jobs): with tablets, refresh streams the sstables into the
# table (load-and-stream), and without tablets, it loads them as they are.
# Translated from scylla-dtest's ics_compaction_test.py::TestIcsCompaction::test_strategy_to_ics_snapshot_refresh,
# which wrote the data with cassandra-stress, in sstables of 1 to 4 MB.
@pytest.mark.parametrize('test_keyspace', ['tablets', 'vnodes'], indirect=True)
@pytest.mark.parametrize('strategy', ['SizeTieredCompactionStrategy', 'LeveledCompactionStrategy',
                                      'TimeWindowCompactionStrategy', 'IncrementalCompactionStrategy'])
def test_refresh_snapshot_into_ics_table(cql, test_keyspace, scylla_data_dir, temp_workdir, scylla_only, strategy):
    check_refresh_snapshot_into_ics_table(cql, test_keyspace, scylla_data_dir, temp_workdir, strategy, truncate=True)

# Like test_refresh_snapshot_into_ics_table, for an ICS table, without
# truncating it after the snapshot, so the snapshot's sstables are loaded
# while the table still has the same sstables, with the same names. As the
# table still has the snapshot's rows too, this test checks that loading
# sstables whose names collide with the table's own works and loses
# nothing - not that the loaded sstables are used.
# Translated from scylla-dtest's ics_compaction_test.py::TestIcsCompaction::test_ics_sstables_refresh_with_collisions,
# which wrote the data with cassandra-stress, in sstables of 2 MB.
@pytest.mark.parametrize('test_keyspace', ['tablets', 'vnodes'], indirect=True)
def test_refresh_snapshot_into_ics_table_without_truncate(cql, test_keyspace, scylla_data_dir, temp_workdir, scylla_only):
    check_refresh_snapshot_into_ics_table(cql, test_keyspace, scylla_data_dir, temp_workdir, 'IncrementalCompactionStrategy', truncate=False)
