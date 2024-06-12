#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with


def test_cleanup(nodetool, scylla_only):
    nodetool("cleanup", expected_requests=[
        expected_request("POST", "/storage_service/cleanup_all", response=0),
    ])


def test_cleanup_keyspace(nodetool):
    nodetool("cleanup", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["ks1", "ks2", "system"]),
        expected_request("POST", "/storage_service/keyspace_cleanup/ks1", response=0),
    ])


def test_cleanup_table(nodetool):
    nodetool("cleanup", "ks1", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["ks1", "ks2", "system"]),
        expected_request("POST", "/storage_service/keyspace_cleanup/ks1", params={"cf": "tbl1"}, response=0),
    ])


def test_cleanup_tables(nodetool):
    nodetool("cleanup", "ks1", "tbl1", "tbl2", "tbl3", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["ks1", "ks2", "system"]),
        expected_request("POST", "/storage_service/keyspace_cleanup/ks1", params={"cf": "tbl1,tbl2,tbl3"}, response=0),
    ])


def test_cleanup_nonexistent_keyspace(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("cleanup", "non_existent_ks"),
            {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2", "system"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])


def test_cleanup_jobs_arg(nodetool):
    nodetool("cleanup", "ks1", "-j", "0", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["ks1", "ks2", "system"]),
        expected_request("POST", "/storage_service/keyspace_cleanup/ks1", response=0),
    ])

    nodetool("cleanup", "ks1", "--jobs", "2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["ks1", "ks2", "system"]),
        expected_request("POST", "/storage_service/keyspace_cleanup/ks1", response=0),
    ])

    nodetool("cleanup", "ks1", "--jobs=1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                         response=["ks1", "ks2", "system"]),
        expected_request("POST", "/storage_service/keyspace_cleanup/ks1", response=0),
    ])
