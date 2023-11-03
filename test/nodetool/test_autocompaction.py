#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request
import utils


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
    utils.check_nodetool_fails_with(
            nodetool,
            ("disableautocompaction", "non_existent_ks"),
            {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])
