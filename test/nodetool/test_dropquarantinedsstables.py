# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with


def test_dropquarantinedsstables_all_keyspaces(nodetool):
    nodetool("dropquarantinedsstables", expected_requests=[
        expected_request("POST", "/storage_service/drop_quarantined_sstables")
    ])


def test_dropquarantinedsstables_one_keyspace(nodetool):
    nodetool("dropquarantinedsstables", "--keyspace", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/drop_quarantined_sstables", params={"keyspace": "ks1"})
    ])


def test_dropquarantinedsstables_one_table(nodetool):
    nodetool("dropquarantinedsstables", "--keyspace", "ks1", "--table", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/drop_quarantined_sstables", params={"keyspace": "ks1", "tables": "tbl1"})
    ])


def test_dropquarantinedsstables_multiple_tables(nodetool):
    nodetool("dropquarantinedsstables", "--keyspace", "ks1", "--table", "tbl1", "--table", "tbl2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/drop_quarantined_sstables", params={"keyspace": "ks1", "tables": "tbl1,tbl2"})
    ])


def test_dropquarantinedsstables_non_existent_keyspace(nodetool):
    check_nodetool_fails_with(
        nodetool,
        ("dropquarantinedsstables", "--keyspace", "non_existent_ks"),
        {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
        ["nodetool: Keyspace [non_existent_ks] does not exist.",
         "error processing arguments: keyspace non_existent_ks does not exist"])


def test_dropquarantinedsstables_keyspace_positional(nodetool):
    nodetool("dropquarantinedsstables", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/drop_quarantined_sstables", params={"keyspace": "ks1"})
    ])


def test_dropquarantinedsstables_keyspace_and_table_positional(nodetool):
    nodetool("dropquarantinedsstables", "ks1", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/drop_quarantined_sstables", params={"keyspace": "ks1", "tables": "tbl1"})
    ])


def test_dropquarantinedsstables_keyspace_and_multiple_tables_positional(nodetool):
    nodetool("dropquarantinedsstables", "ks1", "tbl1", "tbl2", "tbl3", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/drop_quarantined_sstables", params={"keyspace": "ks1", "tables": "tbl1,tbl2,tbl3"})
    ])
