# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import sys
import requests
import threading
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import new_test_table, new_test_keyspace
from test.rest_api.rest_util import scylla_inject_error

def do_test_column_family_attribute_api_table(cql, this_dc, rest_api, api_name):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]

            resp = rest_api.send("GET", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("DELETE", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() == False

            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/{api_name}/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() == True

            # missing table
            resp = rest_api.send("POST", f"column_family/{api_name}/")
            assert resp.status_code == requests.codes.not_found

            # bad syntax 1
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}")
            assert resp.status_code == requests.codes.bad_request
            assert resp.json()['message'] == 'Column family name should be in keyspace:column_family format'

            # bad syntax 2
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}.{test_table}")
            assert resp.status_code == requests.codes.bad_request
            assert resp.json()['message'] == 'Column family name should be in keyspace:column_family format'

            # non-existing keyspace
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}XXX:{test_table}")
            assert resp.status_code == requests.codes.bad_request
            assert "Can't find a column family" in resp.json()['message']

            # non-existing table
            resp = rest_api.send("POST", f"column_family/{api_name}/{test_keyspace}:{test_table}XXX")
            assert resp.status_code == requests.codes.bad_request
            assert "Can't find a column family" in resp.json()['message']

def test_column_family_auto_compaction_table(cql, this_dc, rest_api):
    do_test_column_family_attribute_api_table(cql, this_dc, rest_api, "autocompaction")

def test_column_family_tombstone_gc_api(cql, this_dc, rest_api):
    do_test_column_family_attribute_api_table(cql, this_dc, rest_api, "tombstone_gc")

def test_column_family_tombstone_gc_correctness(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]

            # 1 Allow tombstones to be purged by compaction right away
            cql.execute(f"ALTER TABLE {test_keyspace}.{test_table} with gc_grace_seconds=0")

            # 2 Disable tombstone purge using API
            resp = rest_api.send("DELETE", f"column_family/tombstone_gc/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            resp = rest_api.send("GET", f"column_family/tombstone_gc/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            assert resp.json() == False

            # 3 Create partition tombstone
            cql.execute(f"DELETE from {test_keyspace}.{test_table} WHERE a = 1")

            # 4 Trigger major compaction on that sstable (flushes memtable by default)
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}")
            resp.raise_for_status()

            # 5 Expect that tombstone was not purged
            resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
            resp.raise_for_status()
            assert "-Data.db" in resp.json()[0]

def test_column_family_major_compaction(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}")
            resp.raise_for_status()
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}", params={"flush_memtables": "true"})
            resp.raise_for_status()
            resp = rest_api.send("POST", f"column_family/major_compaction/{test_keyspace}:{test_table}", params={"flush_memtables": "false"})
            resp.raise_for_status()

def test_sstables_by_key_reader_closed(cql, this_dc, rest_api):
    ksdef = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : '1' }}"
    with new_test_keyspace(cql, ksdef) as test_keyspace:
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
            test_table = t.split('.')[1]

            cql.execute(f"INSERT INTO {test_keyspace}.{test_table} (a) VALUES (1)")
            resp = rest_api.send("POST", f"storage_service/keyspace_flush/{test_keyspace}")
            resp.raise_for_status()

            # Check if index reader is closed on happy path.
            resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
            resp.raise_for_status()

            # Check if index reader is closed if exception is thrown.
            with scylla_inject_error(rest_api, "advance_lower_and_check_if_present"):
                resp = rest_api.send("GET", f"column_family/sstables/by_key/{test_keyspace}:{test_table}?key=1")
                assert resp.status_code == 500
