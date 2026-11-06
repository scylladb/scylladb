# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for flushing memtables into sstables, and for finding the sstables
# which contain a key, with Scylla's REST API (which "nodetool flush" and
# "nodetool getsstables" use). So these tests are scylla_only.
#############################################################################

import pytest
import requests

from . import nodetool, rest_api
from .util import new_test_table

# Test that flushing - of all tables, of a keyspace, or of one table - writes
# the table's memtable into a new sstable.
# Translated from scylla-dtest's nodetool_additional_test.py::TestNodetool
# tests test_general_flush, test_keyspace_flush and test_keyspace_column_family_flush.
@pytest.mark.parametrize('scope', ['all', 'keyspace', 'table'])
def test_flush_writes_sstable(cql, test_keyspace, scylla_only, scope):
    with new_test_table(cql, test_keyspace, 'key int PRIMARY KEY, val text') as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES (1, 'asdf')")
        assert rest_api.get_column_family_metric(cql, 'live_ss_table_count', table) == 0
        if scope == 'all':
            nodetool.flush_all(cql)
        elif scope == 'keyspace':
            nodetool.flush_keyspace(cql, test_keyspace)
        else:
            nodetool.flush(cql, table)
        assert rest_api.get_column_family_metric(cql, 'live_ss_table_count', table) == 1

# Test finding the sstables which contain a key, for keys of the types int,
# blob and text: The sstable which the key was flushed to is found, and no
# sstable is found for a key which wasn't written.
# Translated from scylla-dtest's nodetool_additional_test.py::TestNodetool::test_get_sstable.
@pytest.mark.parametrize('key_type,cql_key,key,missing_key', [
    ('int', '4', '4', '5'),
    ('blob', '0x39303138374b4d343830', '39303138374b4d343830', '39303138374b4d343831'),
    ('text', "'keytest'", 'keytest', 'keytest1')],
    ids=['int', 'blob', 'text'])
def test_sstables_by_key(cql, test_keyspace, scylla_only, key_type, cql_key, key, missing_key):
    with new_test_table(cql, test_keyspace, f'key {key_type} PRIMARY KEY, val text') as table:
        cql.execute(f"INSERT INTO {table} (key, val) VALUES ({cql_key}, 'abc')")
        nodetool.flush(cql, table)
        ks, cf = table.split('.')
        def sstables_by_key(key):
            response = requests.get(f'{nodetool.rest_api_url(cql)}/column_family/sstables/by_key/{ks}:{cf}', params={'key': key})
            response.raise_for_status()
            return response.json()
        [sstable] = sstables_by_key(key)
        assert f'/{ks}/{cf}-' in sstable and sstable.endswith('-Data.db')
        assert sstables_by_key(missing_key) == []
