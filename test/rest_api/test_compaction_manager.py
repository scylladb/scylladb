# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

import pytest
import sys
import requests

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
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
