# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import sys

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace
from rest_util import scylla_inject_error

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
