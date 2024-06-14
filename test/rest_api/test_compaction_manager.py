# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import sys
import requests

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import new_test_table, unique_name

# "keyspace" function: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
def new_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute(f"CREATE KEYSPACE {name} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    return name

def test_compaction_manager_stop_compaction(rest_api):
    resp = rest_api.send("POST", "compaction_manager/stop_compaction", { "type": "COMPACTION" })

def test_compaction_manager_stop_keyspace_compaction(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    resp = rest_api.send("POST", f"compaction_manager/stop_keyspace_compaction/{keyspace}", { "type": "COMPACTION" })
    resp.raise_for_status()

    # non-existing keyspace
    resp = rest_api.send("POST", f"compaction_manager/stop_keyspace_compaction/XXX", { "type": "COMPACTION" })
    try:
        resp.raise_for_status()
        pytest.fail("Failed to raise exception")
    except requests.HTTPError as e:
        expected_status_code = requests.codes.bad_request
        assert resp.status_code == expected_status_code, e

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_compaction_manager_stop_keyspace_compaction_tables(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        test_tables = [t0 if not '.' in t0 else t0.split('.')[1]]
        resp = rest_api.send("POST", f"compaction_manager/stop_keyspace_compaction/{keyspace}", { "tables": f"{test_tables[0]}", "type": "COMPACTION" })
        resp.raise_for_status()

        # non-existing table
        resp = rest_api.send("POST", f"compaction_manager/stop_keyspace_compaction/{keyspace}", { "tables": "XXX", "type": "COMPACTION" })
        try:
            resp.raise_for_status()
            pytest.fail("Failed to raise exception")
        except requests.HTTPError as e:
            expected_status_code = requests.codes.bad_request
            assert resp.status_code == expected_status_code, e

        # multiple tables
        with new_test_table(cql, keyspace, "b int, PRIMARY KEY (b)") as t1:
            test_tables += [t1 if not '.' in t1 else t1.split('.')[1]]
            resp = rest_api.send("POST", f"compaction_manager/stop_keyspace_compaction/{keyspace}", { "tables": f"{test_tables[0]},{test_tables[1]}", "type": "COMPACTION" })
            resp.raise_for_status()

            # mixed existing and non-existing tables
            resp = rest_api.send("POST", f"compaction_manager/stop_keyspace_compaction/{keyspace}", { "tables": f"{test_tables[1]},XXX", "type": "COMPACTION" })
            try:
                resp.raise_for_status()
                pytest.fail("Failed to raise exception")
            except requests.HTTPError as e:
                expected_status_code = requests.codes.bad_request
                assert resp.status_code == expected_status_code, e

    cql.execute(f"DROP KEYSPACE {keyspace}")
