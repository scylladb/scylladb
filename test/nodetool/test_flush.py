#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with


# `scylla nodetool flush` invokes the newly added global flush api
def test_flush_all_tables(nodetool, scylla_only):
    nodetool("flush", expected_requests=[
        expected_request("POST", "/storage_service/flush")
    ])


# The java-based `nodetool flush` lists all keyspaces and invoke the per-keyspace flush api on each of them
def test_flush_all_tables_jmx(nodetool, cassandra_only):
    nodetool("flush", expected_requests=[
            expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.MULTIPLE,
                            response=["ks1", "ks2"]),
            expected_request("POST", "/storage_service/keyspace_flush/ks1"),
            expected_request("POST", "/storage_service/keyspace_flush/ks2")
    ])


def test_flush_one_keyspace(nodetool):
    nodetool("flush", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/keyspace_flush/ks1")
    ])


def test_flush_one_table(nodetool):
    nodetool("flush", "ks1", "tbl1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/keyspace_flush/ks1", params={"cf": "tbl1"})
    ])


def test_flush_two_tables(nodetool):
    nodetool("flush", "ks1", "tbl1", "tbl2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"]),
        expected_request("POST", "/storage_service/keyspace_flush/ks1", params={"cf": "tbl1,tbl2"})
    ])


def test_flush_none_existent_keyspace(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("flush", "non_existent_ks"),
            {"expected_requests": [expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
            ["nodetool: Keyspace [non_existent_ks] does not exist.",
             "error processing arguments: keyspace non_existent_ks does not exist"])
