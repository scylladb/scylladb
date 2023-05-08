# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import sys
import requests
import threading
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table, new_test_keyspace

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
