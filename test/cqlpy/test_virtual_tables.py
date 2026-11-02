# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import re
from contextlib import ExitStack
from typing import Tuple

import pytest
from cassandra import WriteFailure
from . import nodetool
from . import rest_api
from . import util
import json
import glob
import os
import subprocess
import time

from collections import defaultdict

from test.pylib.skip_types import skip_env

def verify_snapshots(cql, expected_snapshots: dict[str, set[str]], scylla_data_dir, expiry: int = None):
    results = list(cql.execute(f"SELECT keyspace_name, table_name, snapshot_name, live, total FROM system.snapshots"))
    for res in results:
        if res.snapshot_name in expected_snapshots:
            t = f"{res.keyspace_name}.{res.table_name}"
            assert t in expected_snapshots[res.snapshot_name], f"Unexpected snapshot {t}: snapshot_name={res.snapshot_name}: expected_snapshots={expected_snapshots}"
            expected_snapshots[res.snapshot_name].remove(t)
            verify_snapshot_dir(scylla_data_dir, res.keyspace_name, res.table_name, res.snapshot_name)

    now = time.time()
    if not expiry or int(now) < int(expiry):
        for _, expected_tables in expected_snapshots.items():
            assert not expected_tables, f"Not all expected snapshots were listed: expected_snapshots={expected_snapshots} {now=} {expiry=}"

def snapshot_dir_exists(scylla_data_dir, keyspace_name, table_name, snapshot_name) -> Tuple[str, bool]:
    path = os.path.join(scylla_data_dir, keyspace_name, f"{table_name}-*")
    table_dir = glob.glob(path)
    assert len(table_dir) == 1, f"Expected single table directory for '{path}', got {table_dir}"
    snapshot_dir = os.path.join(table_dir[0], "snapshots", snapshot_name)
    return snapshot_dir, os.path.exists(snapshot_dir)

def verify_snapshot_dir(scylla_data_dir, keyspace_name, table_name, snapshot_name, expected: bool = True):
    snapshot_dir, exists = snapshot_dir_exists(scylla_data_dir, keyspace_name, table_name, snapshot_name)
    if expected:
        assert exists, f"Snapshots directory '{snapshot_dir}' does not exist"
    else:
        assert not exists, f"Snapshots directory '{snapshot_dir}' still exists"

def test_snapshots_table(scylla_only, cql, test_keyspace, scylla_data_dir):
    test_tag = util.unique_name()
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int') as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (0, 0)")
        nodetool.take_snapshot(cql, table, test_tag, False)
        verify_snapshots(cql, {test_tag: {table}}, scylla_data_dir)
        nodetool.del_snapshot(cql, test_tag)
        keyspace_name, table_name = table.split('.')
        verify_snapshot_dir(scylla_data_dir, keyspace_name, table_name, test_tag, False)

@pytest.mark.parametrize("ttl", [0, 5])
def test_snapshots_dropped_table(scylla_only, cql, test_keyspace, scylla_data_dir, ttl):
    cql.execute(f"UPDATE system.config SET value = '{ttl}' WHERE name = 'auto_snapshot_ttl'")
    test_tag = util.unique_name()
    with util.new_test_table(cql, test_keyspace, 'pk int PRIMARY KEY, v int') as table:
        cql.execute(f"INSERT INTO {table} (pk, v) VALUES (0, 0)")
        expiry = int(time.time() + ttl) if ttl else None
        nodetool.take_snapshot(cql, table, test_tag, False, ttl)
        verify_snapshots(cql, {test_tag: {table}}, scylla_data_dir, expiry)
        keyspace_name, table_name = table.split('.')
        if ttl:
            deadline = expiry + 10
            time.sleep(ttl)
            while snapshot_dir_exists(scylla_data_dir, keyspace_name, table_name, test_tag)[1] and time.time() < deadline:
                time.sleep(1)
        else:
            # For ttl == 0, explicitly delete the snapshot and verify removal.
            nodetool.del_snapshot(cql, test_tag)
        verify_snapshot_dir(scylla_data_dir, keyspace_name, table_name, test_tag, False)

def test_snapshots_multiple_keyspaces(scylla_only, cql, scylla_data_dir):
    expected_snapshots = defaultdict(set)
    ks_opts = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    test_tags = [util.unique_name(), util.unique_name(), util.unique_name()]
    with util.new_test_keyspace(cql, ks_opts) as test_keyspace1:
        with util.new_test_table(cql, test_keyspace1, 'pk int PRIMARY KEY, v int') as table1:
            cql.execute(f"INSERT INTO {table1} (pk, v) VALUES (0, 0)")
            nodetool.take_snapshot(cql, table1, test_tags[0], False)
            expected_snapshots[test_tags[0]].add(table1)
            cql.execute(f"INSERT INTO {table1} (pk, v) VALUES (1, 1)")
            nodetool.take_snapshot(cql, table1, test_tags[1], False)
            expected_snapshots[test_tags[1]].add(table1)
            with util.new_test_keyspace(cql, ks_opts) as test_keyspace2:
                with util.new_test_table(cql, test_keyspace2, 'pk int PRIMARY KEY, v int') as table2:
                    cql.execute(f"INSERT INTO {table2} (pk, v) VALUES (0, 0)")
                    nodetool.take_snapshot(cql, table2, test_tags[0], False)
                    expected_snapshots[test_tags[0]].add(table2)
                    cql.execute(f"INSERT INTO {table2} (pk, v) VALUES (2, 2)")
                    nodetool.take_snapshot(cql, table2, test_tags[2], False)
                    expected_snapshots[test_tags[2]].add(table2)

                    verify_snapshots(cql, expected_snapshots, scylla_data_dir)
    for t in test_tags:
        nodetool.del_snapshot(cql, t)

def test_clients(scylla_only, cql):
    columns = ', '.join([
        'address',
        'port',
        'client_type',
        'connection_stage',
        'driver_name',
        'driver_version',
        'hostname',
        'protocol_version',
        'shard_id',
        'ssl_cipher_suite',
        'ssl_enabled',
        'ssl_protocol',
        'username',
        'client_options',
    ])
    cls = list(cql.execute(f"SELECT {columns} FROM system.clients"))
    # There must be at least one connection - the one that sent this SELECT
    # request.
    assert len(cls) > 0
    for cl in cls:
        assert(cl[0] == '127.0.0.1')
        assert(cl[2] == 'cql')
        client_options = cl[13]
        assert(client_options.get('DRIVER_NAME') == cl[4])
        assert(client_options.get('DRIVER_VERSION') == cl[5])

# We only want to check that the table exists with the listed columns, to assert
# backwards compatibility.
def _check_exists(cql, table_name, columns):
    cols = ", ".join(columns)
    assert list(cql.execute(f"SELECT {cols} FROM system.{table_name}"))

def test_protocol_servers(scylla_only, cql):
    _check_exists(cql, "protocol_servers", ("name", "listen_addresses", "protocol", "protocol_version"))

def test_runtime_info(scylla_only, cql):
    _check_exists(cql, "runtime_info", ("group", "item", "value"))

def _runtime_info(cql):
    info = defaultdict(dict)
    for row in cql.execute("SELECT group, item, value FROM system.runtime_info"):
        info[row.group][row.item] = row.value
    return info

# Checks the two values of system.runtime_info that are summed over the tables
# of every shard: the "memtable" group and the "generic.load" item. Both are
# node wide, so the test pins them down with rows whose whereabouts it knows -
# they are accounted for by the memtable group while they are in the memtable,
# and by the load once they have been flushed into an sstable.
def test_runtime_info_reduced_over_tables(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table:
        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)")
        # A handful of partitions is plenty. The size of a row doesn't matter to
        # any of the assertions below, only that the count of them is more than
        # one, so that the sum has to reach the partitions of every shard.
        rows = 10
        for pk in range(rows):
            cql.execute(insert, [pk, pk])

        # The writes are acknowledged only once applied to a memtable, and no
        # amount of data this small can trigger a flush, so all of the rows are
        # still there. The memory is reported in whole logalloc segments, hence
        # non-zero for any amount of data, while the entry count has to account
        # for every partition written.
        memtable = _runtime_info(cql)["memtable"]
        assert int(memtable["entries"]) >= rows
        assert int(memtable["memory_total"]) > 0
        assert int(memtable["memory_used"]) > 0

        # Flushing moves them into an sstable, whose size the load accounts for.
        nodetool.flush(cql, table)
        assert int(_runtime_info(cql)["generic"]["load"]) > 0

def test_versions(scylla_only, cql):
    _check_exists(cql, "versions", ("key", "build_id", "build_mode", "version"))

# Check reading the system.config table, which should list all configuration
# parameters. As we noticed in issue #10047, each type of configuration
# parameter can have a different function for printing it out, and some of
# those may be wrong so we want to check as many as we can - including
# specifically the experimental_features option which was wrong in #10047
# and #11003.
def test_system_config_read(scylla_only, cql):
    # All rows should have the columns name, source, type and value:
    rows = list(cql.execute("SELECT name, source, type, value FROM system.config"))
    values = dict()
    for row in rows:
        values[row.name] = row.value
    # Check that experimental_features exists and makes sense.
    # It needs to be a JSON-formatted strings, and the strings need to be
    # ASCII feature names - not binary garbage as it was in #10047,
    # and not numbers-formatted-as-string as in #11003.
    assert 'experimental_features' in values
    obj = json.loads(values['experimental_features'])
    assert isinstance(obj, list)
    assert isinstance(obj[0], str)
    assert obj[0] and obj[0].isascii() and obj[0].isprintable()
    assert not obj[0].isnumeric()  # issue #11003
    # Check formatting of tri_mode_restriction like
    # restrict_dtcs. These need to be one of
    # allowed string values 0, 1, true, false or warn - but in particular
    # non-empty and printable ASCII, not garbage.
    assert 'restrict_dtcs' in values
    obj = json.loads(values['restrict_dtcs'])
    assert isinstance(obj, str)
    assert obj and obj.isascii() and obj.isprintable()

# Verify that boolean configuration items round-trip and use the yaml/json
# representation (true/false). #19791.
def test_system_config_update_boolean(scylla_only, cql):
    var = 'compaction_enforce_min_threshold'
    value = cql.execute(f"SELECT value FROM system.config WHERE name = '{var}'").one().value
    assert value in ('true', 'false')
    other = 'true' if value == 'false' else 'false'
    cql.execute(f"UPDATE system.config SET value = '{other}' WHERE name = '{var}'")
    readback = cql.execute(f"SELECT value FROM system.config WHERE name = '{var}'").one().value
    assert readback == other

    # just for completeness, check that writing 0/1 works too
    cql.execute(f"UPDATE system.config SET value = '0' WHERE name = '{var}'")
    readback = cql.execute(f"SELECT value FROM system.config WHERE name = '{var}'").one().value
    assert readback == 'false'
    cql.execute(f"UPDATE system.config SET value = '1' WHERE name = '{var}'")
    readback = cql.execute(f"SELECT value FROM system.config WHERE name = '{var}'").one().value
    assert readback == 'true'

    # restore original
    cql.execute(f"UPDATE system.config SET value = '{value}' WHERE name = '{var}'")
    readback = cql.execute(f"SELECT value FROM system.config WHERE name = '{var}'").one().value
    assert readback == value

def test_token_ring_vnodes(scylla_only, cql, test_keyspace_vnodes):
    rows = list(cql.execute(f"SELECT * FROM system.token_ring WHERE keyspace_name = '{test_keyspace_vnodes}'"))
    num_tokens = int(list(cql.execute("SELECT value FROM system.config WHERE name = 'num_tokens'"))[0].value)
    assert len(rows) == num_tokens
    for row in rows:
        assert row.keyspace_name == test_keyspace_vnodes
        assert row.table_name == "<ALL>"

def test_token_ring_tablets(scylla_only, cql, test_keyspace_tablets):
    if test_keyspace_tablets is None:
        skip_env("skipping tablets specific tests -- tablets not enabled")

    with util.new_test_table(cql, test_keyspace_tablets, 'pk int PRIMARY KEY') as table:
        rows = list(cql.execute(f"SELECT * FROM system.token_ring WHERE keyspace_name = '{test_keyspace_tablets}' AND table_name = '{table}'"))
        tablets = list(cql.execute(f"SELECT * from system.tablets WHERE keyspace_name = '{test_keyspace_tablets}' AND table_name = '{table}' ALLOW FILTERING"))
        assert len(rows) == len(tablets)
        for row in rows:
            assert row.keyspace_name == test_keyspace_tablets
            assert row.table_name == table

def test_one_row(scylla_only, cql):
    # The single column is named "system$dummy" — '$' is not a legal
    # character in an unquoted identifier, so user columns cannot clash
    # with it. The driver replaces the '$' with '_' when building the
    # row's attribute names.
    row = cql.execute("SELECT * FROM system.one_row").one()
    assert row.system_dummy == ''
    assert cql.execute("SELECT count(*) FROM system.one_row").one()[0] == 1
    with pytest.raises(WriteFailure, match="virtual table"):
        cql.execute("INSERT INTO system.one_row (\"system$dummy\") VALUES ('Y')")

#############################################################################
# The following tests were translated from scylla-dtest's
# system_tables_test.py.
#############################################################################

# Test that system.token_ring lists a keyspace (with a table, as needed with
# tablets) while it exists, and stops listing it when it is dropped.
# Translated from scylla-dtest's system_tables_test.py::TestTokenRingTable::test_content_one_node_create_and_drop_keyspace.
def test_token_ring_drop_keyspace(scylla_only, cql):
    select = 'SELECT DISTINCT keyspace_name FROM system.token_ring'
    with util.new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        with util.new_test_table(cql, ks, 'p int PRIMARY KEY'):
            assert ks in [r.keyspace_name for r in cql.execute(select)]
    assert ks not in [r.keyspace_name for r in cql.execute(select)]

# Test the content of system.versions: its version is the one the REST API
# reports, and its build id and build mode have the right format.
# test_versions_executable below also compares them with the executable's.
# Translated from scylla-dtest's system_tables_test.py::TestVersionsTable::test_content.
def test_versions_content(scylla_only, cql):
    rows = list(cql.execute("SELECT * FROM system.versions WHERE key = 'local'"))
    assert len(rows) == 1
    row = rows[0]
    assert row.version == rest_api.get_request(cql, 'storage_service/scylla_release_version')
    assert re.fullmatch(r'[0-9a-f]{40}', row.build_id)
    assert row.build_mode in ['release', 'dev', 'debug', 'sanitize', 'coverage']

# Test that system.versions describes the running Scylla executable: its
# version, build id and build mode are the ones which the executable prints
# with --version, --build-id and --build-mode. The scylla_path fixture skips
# this test if Scylla isn't running locally.
# Translated from scylla-dtest's system_tables_test.py::TestVersionsTable::test_content.
def test_versions_executable(scylla_only, cql, scylla_path):
    row = cql.execute("SELECT * FROM system.versions WHERE key = 'local'").one()
    def executable_output(option):
        return subprocess.check_output([scylla_path, option], text=True).strip()
    # Check that each of the three values is the executable's.
    assert row.version == executable_output('--version')
    assert row.build_id == executable_output('--build-id')
    assert row.build_mode == executable_output('--build-mode')

# Test the content of system.protocol_servers: the CQL server's row lists
# its two ports - the regular and the shard-aware one - and the CQL version.
# Other protocol servers, which may or may not be enabled, are not checked.
# Translated from scylla-dtest's system_tables_test.py::TestProtocolServersTable::test_content[default].
# The dtest's variants with Alternator and Redis enabled need a node
# started with them, so they remain in the dtest.
def test_protocol_servers_content(scylla_only, cql):
    def config(name):
        return cql.execute(f"SELECT value FROM system.config WHERE name = '{name}'").one().value.strip('"')
    address = cql.hosts[0].endpoint.address
    cql_version = cql.execute('SELECT cql_version FROM system.local').one().cql_version
    rows = {r.name: r for r in cql.execute('SELECT * FROM system.protocol_servers')}
    assert rows['native transport'].listen_addresses == [f'{address}:{config("native_transport_port")}', f'{address}:{config("native_shard_aware_transport_port")}']
    assert rows['native transport'].protocol == 'cql'
    assert rows['native transport'].protocol_version == cql_version

# Test that a snapshot of a table, or of a whole keyspace, is listed in
# system.snapshots - one row for each of the tables, with numeric sizes -
# and is no longer listed after it is removed.
# Translated from scylla-dtest's system_tables_test.py::TestSnapshotsTable::test_content_create_and_remove_snapshot.
@pytest.mark.parametrize('whole_keyspace', [False, True], ids=['one_table', 'keyspace'])
def test_snapshots_table_create_and_remove(scylla_only, cql, whole_keyspace):
    tag = util.unique_name()
    with util.new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks, ExitStack() as stack:
        tables = [stack.enter_context(util.new_test_table(cql, ks, 'k int PRIMARY KEY, c1 text, c2 text')) for _ in range(5 if whole_keyspace else 1)]
        for table in tables:
            for i in range(10):
                cql.execute(f"INSERT INTO {table} (k, c1, c2) VALUES ({i}, 'value1', 'value2')")
        if whole_keyspace:
            rest_api.post_request(cql, f'storage_service/snapshots?tag={tag}&kn={ks}')
        else:
            nodetool.take_snapshot(cql, tables[0], tag)
        try:
            rows = [r for r in cql.execute('SELECT * FROM system.snapshots') if r.snapshot_name == tag]
            assert sorted(f'{r.keyspace_name}.{r.table_name}' for r in rows) == sorted(tables)
            for r in rows:
                assert isinstance(r.live, int)
                assert isinstance(r.total, int)
        finally:
            nodetool.del_snapshot(cql, tag)
        assert [r for r in cql.execute('SELECT * FROM system.snapshots') if r.snapshot_name == tag] == []

def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

# Test the content of system.runtime_info: the exact set of items in each
# group, the default values of gossip_active and incremental_backup_enabled,
# an uptime of a positive number of seconds, and numbers for all the other
# items. The dtest also compared the memory total with the memory the node
# was started with, which the test doesn't know, so it only checks it is a
# positive number.
# Translated from scylla-dtest's system_tables_test.py::TestRuntimeInfoTable::test_default_content.
def test_runtime_info_default_content(scylla_only, cql):
    expected_items = {
        'cache': {'entries', 'hit_rate_recent', 'hit_rate_total', 'hits', 'memory_free', 'memory_total', 'memory_used', 'misses', 'requests_recent', 'requests_total'},
        'memory': {'free', 'total', 'used'},
        'generic': {'gossip_active', 'incremental_backup_enabled', 'load', 'trace_probability', 'uptime'},
        'memtable': {'entries', 'memory_free', 'memory_total', 'memory_used'},
    }
    info = _runtime_info(cql)
    assert {group: set(items) for group, items in info.items()} == expected_items
    assert info['generic']['gossip_active'] == 'true'
    assert info['generic']['incremental_backup_enabled'] == 'false'
    assert re.fullmatch(r'\d+ seconds', info['generic']['uptime'])
    assert int(info['generic']['uptime'].split()[0]) > 0
    assert int(info['memory']['total']) > 0
    for group, items in info.items():
        for item, value in items.items():
            if (group, item) not in [('generic', 'gossip_active'), ('generic', 'incremental_backup_enabled'), ('generic', 'uptime')]:
                assert is_number(value), f'{group}.{item} = {value}'

# Test that system.runtime_info shows the state of gossip and of
# incremental backups after they are toggled, and after they are restored.
# The dtest toggled them with nodetool; the test uses the same REST API.
# Translated from scylla-dtest's system_tables_test.py::TestRuntimeInfoTable::test_content_toggle_item.
@pytest.mark.parametrize('item,default,changed,change,revert', [
    ('gossip_active', 'true', 'false', ('DELETE', 'storage_service/gossiping'), ('POST', 'storage_service/gossiping')),
    ('incremental_backup_enabled', 'false', 'true', ('POST', 'storage_service/incremental_backups?value=true'), ('POST', 'storage_service/incremental_backups?value=false'))],
    ids=['gossip_active', 'incremental_backup_enabled'])
def test_runtime_info_toggle_item(scylla_only, cql, item, default, changed, change, revert):
    def rest(method, path):
        (rest_api.delete_request if method == 'DELETE' else rest_api.post_request)(cql, path)
    def value():
        return cql.execute(f"SELECT value FROM system.runtime_info WHERE group = 'generic' AND item = '{item}'").one().value
    assert value() == default
    rest(*change)
    try:
        assert value() == changed
    finally:
        rest(*revert)
    assert value() == default

# Test the cache items of system.runtime_info: flushing a table adds its
# rows to the cache, so the number of entries and the memory used grow;
# then reading the table counts hits, but no misses.
# Translated from scylla-dtest's system_tables_test.py::TestRuntimeInfoTable::test_cache_metrics.
def test_runtime_info_cache_metrics(scylla_only, cql, test_keyspace):
    rows = 100
    def cache_metrics():
        return {item: int(value) for item, value in _runtime_info(cql)['cache'].items() if item not in ['hit_rate_recent', 'hit_rate_total']}
    with util.new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, c1 text, c2 text') as table:
        insert = cql.prepare(f"INSERT INTO {table} (k, c1, c2) VALUES (?, 'value1', 'value2')")
        for i in range(rows):
            cql.execute(insert, [i])
        before_flush = cache_metrics()
        nodetool.flush(cql, table)
        after_flush = cache_metrics()
        assert after_flush['entries'] - before_flush['entries'] >= rows
        assert after_flush['memory_used'] > before_flush['memory_used']
        cql.execute(f'SELECT * FROM {table}')
        after_read = cache_metrics()
        assert after_read['requests_total'] > after_flush['requests_total']
        assert after_read['hits'] > after_flush['hits']
        assert after_read['misses'] == after_flush['misses']
        assert after_read['requests_total'] == after_read['hits'] + after_read['misses']

# Test that writing data to a new table increases the memtable entries and
# memory used in system.runtime_info. These are summed over all the tables,
# so to not be confused by memtables of other tests' tables shrinking, the
# test first flushes all memtables. The dtest, which ran on a new node, was
# an xfail test for issue #10340 (the memtable items were always zero); it
# was fixed by commit c2e954adea.
# Translated from scylla-dtest's system_tables_test.py::TestRuntimeInfoTable::test_memtable_metrics.
def test_runtime_info_memtable_metrics(scylla_only, cql, test_keyspace):
    def memtable_metrics():
        return {item: int(value) for item, value in _runtime_info(cql)['memtable'].items()}
    nodetool.flush_all(cql)
    before = memtable_metrics()
    with util.new_test_table(cql, test_keyspace, 'k int PRIMARY KEY, c1 text, c2 text') as table:
        insert = cql.prepare(f"INSERT INTO {table} (k, c1, c2) VALUES (?, 'value1', 'value2')")
        for i in range(100):
            cql.execute(insert, [i])
        after = memtable_metrics()
        assert after['entries'] > before['entries']
        assert after['memory_used'] > before['memory_used']

# Test that the value of each option in system.config matches its type.
# Translated from scylla-dtest's system_tables_test.py::TestConfigTable::test_content_values_types.
def test_system_config_values_types(scylla_only, cql):
    for row in cql.execute('SELECT * FROM system.config'):
        if row.type in ['integer', 'double', 'float']:
            assert is_number(row.value), row
        elif row.type == 'bool':
            assert row.value.lower() in ['true', 'false'], row
        elif row.type == 'string list':
            assert re.match(r'\[.*]', row.value), row
        elif row.type == 'string map':
            assert re.match(r'\{.*}', row.value), row
        elif row.type == 'string':
            assert row.value.isprintable(), row
        else:
            assert row.value, row

# Test the errors of invalid updates of system.config: without a value,
# of the source or the type, of an option that isn't live-updatable or
# doesn't exist, and with a value of the wrong type (which used to cause an
# error in the log - issue #10394; since its fix, the error is "cannot parse
# option value").
# Translated from scylla-dtest's system_tables_test.py::TestConfigTable::test_invalid_update.
@pytest.mark.parametrize('statement,error', [
    ("SET source = 'default' WHERE name = 'api_port'", 'option value is required'),
    ("SET source = 'default', value = '15000' WHERE name = 'api_port'", 'option source is not updateable'),
    ("SET type = 'bool', value = '15000' WHERE name = 'api_port'", 'option type is immutable'),
    ("SET value = '15000' WHERE name = 'api_port'", 'option is not live-updateable'),
    ("SET value = '15000' WHERE name = 'some_generic_name'", 'no such option'),
    ("SET value = 'true' WHERE name = 'failure_detector_timeout_in_ms'", 'cannot parse option value')],
    ids=['no_value_provided', 'source_not_updatable', 'type_not_updatable', 'parameter_not_live_updatable', 'wrong_parameter_name', 'wrong_value_type'])
def test_system_config_invalid_update(scylla_only, cql, statement, error):
    with pytest.raises(WriteFailure, match=re.escape(error)):
        cql.execute(f'UPDATE system.config {statement}')

# Test updating live-updatable options in system.config: after the update,
# the option has the new value, and its source is "cql". Unlike the dtest,
# the test restores each option's value after checking it, because the
# Scylla it runs on is shared with other tests.
# Translated from scylla-dtest's system_tables_test.py::TestConfigTable::test_content_update_value.
def test_system_config_update_value(scylla_only, cql):
    options = {
        'compaction_enforce_min_threshold': 'true',
        'failure_detector_timeout_in_ms': '30000',
        'max_hinted_handoff_concurrency': '10',
        'enable_repair_based_node_ops': 'false',
        'allowed_repair_based_node_ops': 'some_value',
        'force_gossip_generation': '5',
        'abort_on_internal_error': 'true',
        'max_partition_key_restrictions_per_query': '150',
        'max_clustering_key_restrictions_per_query': '150',
        'max_memory_for_unlimited_query_soft_limit': '1572864',
        'max_memory_for_unlimited_query_hard_limit': '157286400',
        'max_concurrent_requests_per_shard': '430000000',
        'flush_schema_tables_after_modification': 'false',
    }
    for name, value in options.items():
        with util.config_value_context(cql, name, '0' if value == 'false' else '1' if value == 'true' else value):
            row = cql.execute(f"SELECT * FROM system.config WHERE name = '{name}'").one()
            assert row.value.strip('"') == value
            assert row.source == 'cql'
