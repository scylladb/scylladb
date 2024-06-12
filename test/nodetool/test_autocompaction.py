#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with

def test_disableautocompaction(nodetool):
    nodetool("disableautocompaction", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request("DELETE", "/storage_service/auto_compaction/ks1"),
        expected_request("DELETE", "/storage_service/auto_compaction/ks2")
    ])


def test_disableautocompaction_one_keyspace(nodetool):
    nodetool("disableautocompaction", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/auto_compaction/ks1")
    ])


def test_disableautocompaction_one_table(nodetool):
    nodetool("disableautocompaction", "ks1", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/auto_compaction/ks1", params={"cf": "tbl1"})
    ])


def test_disableautocompaction_two_tables(nodetool):
    nodetool("disableautocompaction", "ks1", "tbl1", "tbl2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/auto_compaction/ks1", params={"cf": "tbl1,tbl2"})
    ])


def test_disableautocompaction_none_existent_keyspace(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("disableautocompaction", "non_existent_ks"),
            {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])


def test_enableautocompaction(nodetool):
    nodetool("enableautocompaction", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"],
                         multiple=expected_request.MULTIPLE),
        expected_request("POST", "/storage_service/auto_compaction/ks1"),
        expected_request("POST", "/storage_service/auto_compaction/ks2")
    ])


def test_enableautocompaction_one_keyspace(nodetool):
    nodetool("enableautocompaction", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/auto_compaction/ks1")
    ])


def test_enableautocompaction_one_table(nodetool):
    nodetool("enableautocompaction", "ks1", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/auto_compaction/ks1", params={"cf": "tbl1"})
    ])


def test_enableautocompaction_two_tables(nodetool):
    nodetool("enableautocompaction", "ks1", "tbl1", "tbl2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/auto_compaction/ks1", params={"cf": "tbl1,tbl2"})
    ])


def test_enableautocompaction_none_existent_keyspace(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("enableautocompaction", "non_existent_ks"),
            {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])
