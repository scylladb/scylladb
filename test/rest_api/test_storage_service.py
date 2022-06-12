# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import sys
import requests

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import unique_name, new_test_table, new_test_keyspace, new_materialized_view, new_secondary_index
from rest_util import new_test_snapshot

# "keyspace" function: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
def new_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute(f"CREATE KEYSPACE {name} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    return name

def test_storage_service_auto_compaction_keyspace(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    # test empty keyspace
    resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}")
    resp.raise_for_status()

    resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}")
    resp.raise_for_status()

    # test non-empty keyspace
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t:
        resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}")
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}")
        resp.raise_for_status()

        # non-existing keyspace
        resp = rest_api.send("POST", f"storage_service/auto_compaction/XXX")
        assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_table(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t:
        test_table = t.split('.')[1]
        resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}", { "cf": test_table })
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": test_table })
        resp.raise_for_status()

        # non-existing table
        resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": "XXX" })
        assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_auto_compaction_tables(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables = [t0.split('.')[1], t1.split('.')[1]]
            resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/auto_compaction/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
            assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_offstrategy_compaction(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}")
        resp.raise_for_status()

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_keyspace_offstrategy_compaction_tables(cql, this_dc, rest_api):
    keyspace = new_keyspace(cql, this_dc)
    with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t0:
        with new_test_table(cql, keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables = [t0.split('.')[1], t1.split('.')[1]]

            resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/keyspace_offstrategy_compaction/{keyspace}", { "cf": f"{test_tables[0]},XXX" })
            assert resp.status_code == requests.codes.bad_request

    cql.execute(f"DROP KEYSPACE {keyspace}")

def test_storage_service_snapshot(cql, this_dc, rest_api):
    resp = rest_api.send("GET", "storage_service/snapshots")
    resp.raise_for_status()

    def verify_snapshot_details(expected):
        resp = rest_api.send("GET", "storage_service/snapshots")
        found = False
        for data in resp.json():
            if data['key'] == expected['key']:
                assert not found
                found = True
                sort_key = lambda v: f"{v['ks']}-{v['cf']}"
                value = sorted([v for v in data['value'] if not v['ks'].startswith('system')], key=sort_key)
                expected_value = sorted(expected['value'], key=sort_key)
                assert len(value) == len(expected_value), f"length mismatch: expected {expected_value} but got {value}"
                for i in range(len(value)):
                    v = value[i]
                    # normalize `total` and `live`
                    # since we care only if they are zero or not
                    v['total'] = 1 if v['total'] else 0
                    v['live'] = 1 if v['live'] else 0
                    ev = expected_value[i]
                    assert v == ev
        assert found, f"key='{expected['key']}' not found in {resp.json()}"

    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace0:
        with new_test_table(cql, keyspace0, "p text PRIMARY KEY") as table00:
            ks0, cf00 = table00.split('.')
            stmt = cql.prepare(f"INSERT INTO {table00} (p) VALUES (?)")
            cql.execute(stmt, ["pk0"])

            # single keyspace / table
            with new_test_snapshot(rest_api, ks0, cf00) as snapshot0:
                verify_snapshot_details({
                    'key': snapshot0,
                    'value': [{'ks': ks0, 'cf': cf00, 'total': 1, 'live': 0}]
                })

                cql.execute(f"TRUNCATE {table00}")
                verify_snapshot_details({
                    'key': snapshot0,
                    'value': [{'ks': ks0, 'cf': cf00, 'total': 1, 'live': 1}]
                })

            with new_test_table(cql, keyspace0, "p text PRIMARY KEY") as table01:
                _, cf01 = table01.split('.')
                stmt = cql.prepare(f"INSERT INTO {table01} (p) VALUES (?)")
                cql.execute(stmt, ["pk1"])

                # single keyspace / multiple tables
                with new_test_snapshot(rest_api, ks0, [cf00, cf01]) as snapshot1:
                    verify_snapshot_details({
                        'key': snapshot1,
                        'value': [
                            {'ks': ks0, 'cf': cf00, 'total': 0, 'live': 0},
                            {'ks': ks0, 'cf': cf01, 'total': 1, 'live': 0}
                        ]
                    })

                with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace1:
                    with new_test_table(cql, keyspace1, "p text PRIMARY KEY") as table10:
                        ks1, cf10 = table10.split('.')

                        # multiple keyspaces
                        with new_test_snapshot(rest_api, [ks0, ks1]) as snapshot2:
                            verify_snapshot_details({
                                'key': snapshot2,
                                'value': [
                                    {'ks': ks0, 'cf': cf00, 'total': 0, 'live': 0},
                                    {'ks': ks0, 'cf': cf01, 'total': 1, 'live': 0},
                                    {'ks': ks1, 'cf': cf10, 'total': 0, 'live': 0}
                                ]
                            })

                        # all keyspaces
                        with new_test_snapshot(rest_api, ) as snapshot3:
                            verify_snapshot_details({
                                'key': snapshot3,
                                'value': [
                                    {'ks': ks0, 'cf': cf00, 'total': 0, 'live': 0},
                                    {'ks': ks0, 'cf': cf01, 'total': 1, 'live': 0},
                                    {'ks': ks1, 'cf': cf10, 'total': 0, 'live': 0}
                                ]
                            })

# Verify that snapshots of materialized views and secondary indexes are disallowed.
def test_storage_service_snapshot_mv_si(cql, this_dc, rest_api):
    resp = rest_api.send("GET", "storage_service/snapshots")
    resp.raise_for_status()

    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as table:
            with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
                try:
                    with new_test_snapshot(rest_api, keyspace, mv.split('.')[1]) as snap:
                        pytest.fail(f"Snapshot of materialized view {mv} should have failed")
                except requests.HTTPError:
                    pass

            with new_secondary_index(cql, table, 'v') as si:
                try:
                    with new_test_snapshot(rest_api, keyspace, si.split('.')[1]) as snap:
                        pytest.fail(f"Snapshot of secondary index {si} should have failed")
                except requests.HTTPError:
                    pass

# ...but not when the snapshot is automatic (pre-scrub).
def test_materialized_view_pre_scrub_snapshot(cql, this_dc, rest_api):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as keyspace:
        schema = 'p int, v text, primary key (p)'
        with new_test_table(cql, keyspace, schema) as table:
            stmt = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
            cql.execute(stmt, [0, 'hello'])

            with new_materialized_view(cql, table, '*', 'v, p', 'v is not null and p is not null') as mv:
                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}")
                resp.raise_for_status()

            with new_secondary_index(cql, table, 'v') as si:
                resp = rest_api.send("GET", f"storage_service/keyspace_scrub/{keyspace}")
                resp.raise_for_status()
