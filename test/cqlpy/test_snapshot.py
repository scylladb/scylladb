# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import glob
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager

import pytest
import requests

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

#############################################################################
# The following tests were translated from scylla-dtest's snapshot_test.py.
# They are scylla_only because they use Scylla's REST API, and most of them
# also read and write files in Scylla's data directory.
#############################################################################

# Send a request to Scylla's REST API, and check that it succeeded.
def rest_request(cql, method, path, **params):
    response = requests.request(method, f'{nodetool.rest_api_url(cql)}/{path}', params=params)
    assert response.ok, response.text

# Split a table name "ks.cf" into the keyspace and table names. The table
# name may be quoted.
def split_table(table):
    ks, cf = table.split('.')
    return ks, cf.strip('"')

# Take a snapshot of the given tables, all in the same keyspace.
def snapshot_tables(cql, tables, tag, **params):
    ks = split_table(tables[0])[0]
    rest_request(cql, 'POST', 'storage_service/snapshots', tag=tag, kn=ks,
                 cf=','.join(split_table(table)[1] for table in tables), **params)

# Return the data directory of the given table, "<data_dir>/<ks>/<cf>-<id>".
# The directory of a dropped table may remain (e.g., if it has snapshots),
# and a new table with the same name has a different directory, so we find
# the directory by the table's id.
def table_dir(cql, data_dir, table):
    ks, cf = split_table(table)
    id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{cf}'").one().id
    return os.path.join(data_dir, ks, f'{cf}-{id.hex}')

def read_schema_file(cql, data_dir, table, tag):
    with open(os.path.join(table_dir(cql, data_dir, table), 'snapshots', tag, 'schema.cql')) as f:
        return f.read()

def describe_table(cql, table):
    return cql.execute(f'DESCRIBE TABLE {table}').one().create_statement

# Copy the given table's snapshot to a new directory under workdir, like a
# backup would, and return the copy's path.
def save_snapshot(cql, data_dir, table, tag, workdir):
    saved = os.path.join(workdir, *split_table(table))
    shutil.copytree(os.path.join(table_dir(cql, data_dir, table), 'snapshots', tag), saved)
    return saved

# Load the sstables of a saved snapshot into the given table: Copy them into
# the table's "upload" directory, and ask Scylla to load them, like
# "nodetool refresh" does.
def restore_with_refresh(cql, data_dir, saved, table):
    upload_dir = os.path.join(table_dir(cql, data_dir, table), 'upload')
    for f in os.listdir(saved):
        if f not in ['manifest.json', 'schema.cql']:
            shutil.copy(os.path.join(saved, f), upload_dir)
    ks, cf = split_table(table)
    rest_request(cql, 'POST', f'storage_service/sstables/{ks}', cf=cf)

# Test restoring tables from a snapshot: Take a snapshot of a keyspace with
# two tables, write more rows, drop the tables and create them again, and
# then copy the snapshot's sstables into the new tables and load them. The
# tables should have exactly the rows written before the snapshot.
# Translated from scylla-dtest's snapshot_test.py::TestSnapshot tests
# test_basic_snapshot_and_restore_with_refresh (a snapshot of a keyspace),
# and test_basic_mulitple_tables_snapshot_and_restore_with_refresh,
# test_basic_mulitple_tables_snapshot_using_column_family and
# test_basic_mulitple_tables_snapshot_using_table (a snapshot of a list of
# tables, given to "nodetool snapshot" with its -cf, --column-family and
# --table options). The translation of these nodetool options to the REST
# API is checked by test/nodetool's test_snapshot_keyspace_with_table, and
# snapshots of a list of tables by test/rest_api's
# test_storage_service_snapshot.
def test_snapshot_restore_with_refresh(cql, scylla_data_dir, temp_workdir, scylla_only):
    with keyspaces_with_tables(cql, 1, 2) as tables:
        [(ks, ks_tables)] = tables.items()
        tag = unique_name()
        rest_request(cql, 'POST', 'storage_service/snapshots', tag=tag, kn=ks)
        try:
            saved = [save_snapshot(cql, scylla_data_dir, table, tag, temp_workdir) for table in ks_tables]
        finally:
            nodetool.del_snapshot(cql, tag)
        # Write another row after the snapshot. The restore should not have it.
        for table in ks_tables:
            cql.execute(f"INSERT INTO {table} (key, val) VALUES (2, 'asdf')")
        for table, saved_dir in zip(ks_tables, saved):
            cql.execute(f'DROP TABLE {table}')
            cql.execute(f'CREATE TABLE {table} (key int PRIMARY KEY, val text)')
            assert list(cql.execute(f'SELECT * FROM {table}')) == []
            restore_with_refresh(cql, scylla_data_dir, saved_dir, table)
            assert list(cql.execute(f'SELECT * FROM {table}')) == [(1, 'asdf')]

# Test that a snapshot request may not list tables together with more than
# one keyspace.
# Translated from scylla-dtest's snapshot_test.py::TestSnapshot::test_snapshot_for_2kc_and_cf_failure.
def test_snapshot_tables_of_two_keyspaces(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        ks, cf = split_table(table)
        response = requests.post(f'{nodetool.rest_api_url(cql)}/storage_service/snapshots',
                                 params={'tag': unique_name(), 'kn': f'{ks},system', 'cf': cf})
        assert response.status_code == 400
        assert 'Only one keyspace allowed when specifying a column family' in response.json()['message']

# Test running snapshot operations concurrently: taking snapshots of tables
# and of all keyspaces, deleting the snapshots of some keyspaces and of all
# keyspaces, and listing snapshots. None of them should fail.
# Translated from scylla-dtest's snapshot_test.py::TestParallelSnapshotOperations
# tests test_parallel_creating_cleaning_one_ks,
# test_parallel_operations_for_10_ks_1_table_per_ks,
# test_parallel_operations_for_10ks_and_10tables_and_clearallsnapshots,
# test_parallel_operation_create_clear_for_all_ks,
# test_parallel_operations_create_clear_per_ks_and_all and
# test_parallel_operations_create_list_clear_for_all_ks. These dtests used
# up to 30 keyspaces of 10 tables each, and ran each operation once or twice;
# this test uses 2 keyspaces of 2 tables, and runs each operation several
# times, so the operations overlap. The dtests also deleted all snapshots
# ("nodetool clearsnapshot") - this test only deletes all keyspaces'
# snapshots with its own tags, so it doesn't delete snapshots it didn't take.
def test_snapshot_parallel_operations(cql, scylla_only):
    with keyspaces_with_tables(cql, 2, 2) as tables:
        prefix = unique_name()
        repeats = 2
        def snapshot_each_table(i):
            for table in [table for ks_tables in tables.values() for table in ks_tables]:
                snapshot_tables(cql, [table], f'{prefix}_tables_{i}')
        def delete_keyspaces_snapshots(i):
            rest_request(cql, 'DELETE', 'storage_service/snapshots', kn=','.join(tables))
        def snapshot_all_keyspaces(i):
            rest_request(cql, 'POST', 'storage_service/snapshots', tag=f'{prefix}_all_{i}')
        def delete_all_keyspaces_snapshot(i):
            rest_request(cql, 'DELETE', 'storage_service/snapshots', tag=f'{prefix}_all_{i}')
        def list_snapshots(i):
            rest_request(cql, 'GET', 'storage_service/snapshots')
        operations = [snapshot_each_table, delete_keyspaces_snapshots, snapshot_all_keyspaces,
                      delete_all_keyspaces_snapshot, list_snapshots]
        start = threading.Barrier(len(operations))
        def run(operation):
            start.wait()
            for i in range(repeats):
                operation(i)
        try:
            with ThreadPoolExecutor(max_workers=len(operations)) as pool:
                for future in [pool.submit(run, operation) for operation in operations]:
                    future.result()
        finally:
            for i in range(repeats):
                nodetool.del_snapshot(cql, f'{prefix}_tables_{i}')
                nodetool.del_snapshot(cql, f'{prefix}_all_{i}')

# Take a snapshot of the given tables (in the same keyspace), and check that
# each one's "schema.cql" file in the snapshot has a CREATE TABLE statement
# which creates the table again, with the same schema (as shown by DESCRIBE),
# after it is dropped.
def check_schema_files(cql, data_dir, tables):
    descriptions = [describe_table(cql, table) for table in tables]
    tag = unique_name()
    snapshot_tables(cql, tables, tag)
    try:
        schemas = [read_schema_file(cql, data_dir, table, tag) for table in tables]
    finally:
        nodetool.del_snapshot(cql, tag)
    for table, schema, description in zip(tables, schemas, descriptions):
        cql.execute(f'DROP TABLE {table}')
        cql.execute(schema)
        assert describe_table(cql, table) == description

# Test that a snapshot of two tables has a schema file for each one, which
# creates it again after it is dropped.
# Translated from scylla-dtest's snapshot_test.py::TestSchemaFileInSnapshot
# tests test_schema_file_created and test_schema_file_created_by_multiple_tables.
def test_snapshot_schema_file(cql, test_keyspace, scylla_data_dir, scylla_only):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table1, \
         new_test_table(cql, test_keyspace, 'p int, c text, v list<int>, PRIMARY KEY (p, c)',
                        "WITH CLUSTERING ORDER BY (c DESC) AND comment = 'hello'") as table2:
        check_schema_files(cql, scylla_data_dir, [table1, table2])

# Test that the schema file of a table with quoted upper-case table and
# column names creates the table again with these names. The snapshot also
# has a table with a lower-case name.
# Translated from scylla-dtest's snapshot_test.py::TestSchemaFileInSnapshot
# tests test_upper_case_of_table_name_is_saved and
# test_upper_case_of_table_name_is_saved_mixed_case.
def test_snapshot_schema_file_quoted_names(cql, test_keyspace, scylla_data_dir, scylla_only):
    upper = f'{test_keyspace}."{unique_name().upper()}"'
    cql.execute(f'CREATE TABLE {upper} ("KEY" int PRIMARY KEY, "VAL" text)')
    try:
        with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as lower:
            check_schema_files(cql, scylla_data_dir, [upper, lower])
    finally:
        cql.execute(f'DROP TABLE IF EXISTS {upper}')

# Test that a snapshot's schema file has the table's schema at the time of
# the snapshot: After ALTER TABLE adds a column, a new snapshot's schema file
# has the new column, and creates the altered table.
# Translated from scylla-dtest's snapshot_test.py::TestSchemaFileInSnapshot::test_schema_file_contains_altering_table_changes.
def test_snapshot_schema_file_after_alter(cql, test_keyspace, scylla_data_dir, scylla_only):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        tag1 = unique_name()
        tag2 = unique_name()
        description1 = describe_table(cql, table)
        snapshot_tables(cql, [table], tag1)
        cql.execute(f'ALTER TABLE {table} ADD val1 text')
        description2 = describe_table(cql, table)
        snapshot_tables(cql, [table], tag2)
        try:
            schema1 = read_schema_file(cql, scylla_data_dir, table, tag1)
            schema2 = read_schema_file(cql, scylla_data_dir, table, tag2)
        finally:
            nodetool.del_snapshot(cql, tag1)
            nodetool.del_snapshot(cql, tag2)
        assert 'val1' not in schema1
        assert 'val1' in schema2
        cql.execute(f'DROP TABLE {table}')
        cql.execute(schema2)
        assert describe_table(cql, table) == description2
        assert description2 != description1

# Three values of each native type, as CQL literals or functions.
native_type_values = {
    'bigint': ['10000', '1', '2'],
    'boolean': ['true', 'true', 'false'],
    'blob': ["textAsBlob('1234567890qwertyuiop')", "textAsBlob('a')", 'bigintAsBlob(1)'],
    'date': ['currentDate()', 'currentDate()', 'currentDate()'],
    'decimal': ['10.1', '11.1', '12.2'],
    'double': ['10.1000001', '11.111111', '22.22222'],
    'duration': ['89h1m48s', '11h11m11s', '22h22m22s'],
    'float': ['10.10001', '33.33', '44.44'],
    'inet': ["'1.1.1.1'", "'1.1.1.1'", "'2.2.2.2'"],
    'int': ['100001', '1', '2'],
    'smallint': ['1', '1', '2'],
    'time': ['currentTime()', 'currentTime()', 'currentTime()'],
    'timestamp': ['currentTimestamp()', 'currentTimestamp()', 'currentTimestamp()'],
    'timeuuid': ['currentTimeUUID()', 'currentTimeUUID()', 'currentTimeUUID()'],
    'tinyint': ['1', '2', '5'],
    'uuid': ['uuid()', 'uuid()', 'uuid()'],
    'varint': ['1', '4', '5'],
    'text': ["'a'", "'A'", "'b'"],
    'varchar': ["'c'", "'d'", "'E'"],
    'ascii': ["'1'", "'c'", "'$'"],
}

# Test restoring a table from a snapshot, using the snapshot's schema file to
# create the table again and then loading the snapshot's sstables into it.
# The table has a column of each native type - or a UDT, frozen or not, with
# a field of each native type - to check that values of all types are
# restored. The schema file doesn't create the UDT, so the test creates the
# UDT again before restoring the table.
# Translated from scylla-dtest's snapshot_test.py::TestSchemaFileInSnapshot
# tests test_restoring_by_schema_file_with_refresh,
# test_restore_data_for_all_native_data_types_from_snapshot_with_refresh,
# test_restore_data_from_snapshot_with_udt_with_refresh and
# test_restore_data_from_snapshot_with_frozen_udt_with_refresh. The dtests
# only checked the number of restored rows; this test checks their values.
@pytest.mark.parametrize('kind', ['columns', 'udt', 'frozen_udt'])
def test_snapshot_restore_native_types(cql, scylla_data_dir, temp_workdir, scylla_only, kind):
    with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        types = list(native_type_values)
        fields = ', '.join(f'f_{t} {t}' for t in types)
        if kind == 'columns':
            schema = f'key bigint PRIMARY KEY, {fields}'
        else:
            cql.execute(f'CREATE TYPE {ks}.all_types ({fields})')
            schema = 'key bigint PRIMARY KEY, u ' + ('frozen<all_types>' if kind == 'frozen_udt' else 'all_types')
        table = f'{ks}.{unique_name()}'
        cql.execute(f'CREATE TABLE {table} ({schema})')
        def insert(key, i):
            values = [native_type_values[t][i] for t in types]
            if kind == 'columns':
                columns = ', '.join(f'f_{t}' for t in types)
                cql.execute(f'INSERT INTO {table} (key, {columns}) VALUES ({key}, {", ".join(values)})')
            else:
                udt = ', '.join(f'f_{t}: {v}' for t, v in zip(types, values))
                cql.execute(f'INSERT INTO {table} (key, u) VALUES ({key}, {{{udt}}})')
        def read_rows():
            return sorted(cql.execute(f'SELECT * FROM {table}'), key=lambda row: row.key)
        for i in range(3):
            insert(i, i)
        rows = read_rows()
        description = describe_table(cql, table)
        tag = unique_name()
        snapshot_tables(cql, [table], tag)
        try:
            saved = save_snapshot(cql, scylla_data_dir, table, tag, temp_workdir)
        finally:
            nodetool.del_snapshot(cql, tag)
        # Write another row after the snapshot. The restore should not have it.
        insert(3, 0)
        cql.execute(f'DROP TABLE {table}')
        if kind != 'columns':
            cql.execute(f'DROP TYPE {ks}.all_types')
            cql.execute(f'CREATE TYPE {ks}.all_types ({fields})')
        with open(os.path.join(saved, 'schema.cql')) as f:
            cql.execute(f.read())
        assert describe_table(cql, table) == description
        assert read_rows() == []
        restore_with_refresh(cql, scylla_data_dir, saved, table)
        assert read_rows() == rows

# Test that a snapshot request which doesn't list keyspaces takes a snapshot
# of every keyspace which has tables, including the system keyspaces.
# Translated from scylla-dtest's snapshot_test.py::TestSnapshotOptions::test_snapshot_defaults_to_all_keyspaces.
def test_snapshot_all_keyspaces(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        tag = unique_name()
        rest_request(cql, 'POST', 'storage_service/snapshots', tag=tag)
        try:
            entries = has_snapshot(nodetool.list_snapshots(cql), tag)
        finally:
            nodetool.del_snapshot(cql, tag)
        keyspaces = {row.keyspace_name for row in cql.execute('SELECT keyspace_name FROM system_schema.tables')}
        assert {e['ks'] for e in entries} == keyspaces
        assert split_table(table) in {(e['ks'], e['cf']) for e in entries}

# Test that a snapshot request which lists keyspaces takes a snapshot of
# only these keyspaces.
# Translated from scylla-dtest's snapshot_test.py::TestSnapshotOptions tests
# test_snapshot_of_specified_keyspaces_only and
# test_snapshot_of_specified_keyspaces_only_with_kc_list_option. The latter
# gave the keyspaces to "nodetool snapshot" with its -kc option, whose
# translation to the REST API is checked by test/nodetool's test_snapshot_ktlist.
def test_snapshot_some_keyspaces(cql, scylla_only):
    with keyspaces_with_tables(cql, 3, 1) as tables:
        ks1, ks2, _ = tables
        tag = unique_name()
        rest_request(cql, 'POST', 'storage_service/snapshots', tag=tag, kn=f'{ks1},{ks2}')
        try:
            entries = has_snapshot(nodetool.list_snapshots(cql), tag)
        finally:
            nodetool.del_snapshot(cql, tag)
        assert sorted(f"{e['ks']}.{e['cf']}" for e in entries) == sorted(tables[ks1] + tables[ks2])

# Test that a snapshot with the "skip flush" option doesn't flush the
# memtable, so it doesn't have the data which is only in the memtable, while
# a snapshot without this option flushes the memtable and has its data.
# Translated from scylla-dtest's snapshot_test.py::TestSnapshotOptions::test_snapshot_skip_flush.
# The dtest checked that the memtable's size and number of flushes didn't
# change; this test checks directly that the snapshot has no sstables.
def test_snapshot_skip_flush(cql, test_keyspace, scylla_data_dir, scylla_only):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, 'asdf')")
        tag1 = unique_name()
        tag2 = unique_name()
        snapshot_tables(cql, [table], tag1, sf='true')
        snapshot_tables(cql, [table], tag2)
        try:
            snapshots_dir = os.path.join(table_dir(cql, scylla_data_dir, table), 'snapshots')
            assert os.path.isdir(os.path.join(snapshots_dir, tag1))
            assert glob.glob(os.path.join(snapshots_dir, tag1, '*-Data.db')) == []
            assert glob.glob(os.path.join(snapshots_dir, tag2, '*-Data.db')) != []
        finally:
            nodetool.del_snapshot(cql, tag1)
            nodetool.del_snapshot(cql, tag2)
