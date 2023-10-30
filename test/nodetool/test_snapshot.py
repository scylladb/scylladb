#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request
import utils


def test_clearnapshot(nodetool):
    nodetool("clearsnapshot", expected_requests=[
        expected_request("DELETE", "/storage_service/snapshots")
    ])


def test_clearnapshot_keyspace(nodetool):
    nodetool("clearsnapshot", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1"})
    ])


def test_clearnapshot_keyspaces(nodetool):
    nodetool("clearsnapshot", "ks1", "ks2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1,ks2"})
    ])


def test_clearnapshot_nonexistent_keyspaces(nodetool, scylla_only):
    utils.check_nodetool_fails_with(
            nodetool,
            ("clearsnapshot", "non_existent_ks"),
            {"expected_requests": [
                expected_request("GET", "/storage_service/keyspaces", response=["ks1", "ks2"])]},
            ["error processing arguments: keyspace non_existent_ks does not exist"])


def test_clearnapshot_tag(nodetool):
    nodetool("clearsnapshot", "-t", "snapshot_name", expected_requests=[
        expected_request("DELETE", "/storage_service/snapshots", params={"tag": "snapshot_name"})
    ])


def test_clearnapshot_tag_and_keyspace(nodetool):
    nodetool("clearsnapshot", "-t", "snapshot_name", "ks1", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1", "tag": "snapshot_name"})
    ])


def test_clearnapshot_tag_and_keyspaces(nodetool):
    nodetool("clearsnapshot", "-t", "snapshot_name", "ks1", "ks2", expected_requests=[
        expected_request("GET", "/storage_service/keyspaces", multiple=expected_request.ANY,
                         response=["ks1", "ks2"]),
        expected_request("DELETE", "/storage_service/snapshots", params={"kn": "ks1,ks2", "tag": "snapshot_name"})
    ])


def test_listsnapshots(nodetool, request):
    res = nodetool("listsnapshots", expected_requests=[
        expected_request("GET", "/storage_service/snapshots", response=[
            {"key": "1698236289867", "value": [{"ks": "ks1", "cf": "tbl1", "total": 45056, "live": 0},
                                               {"ks": "ks1", "cf": "tbl2", "total": 40956, "live": 0}]},
            {"key": "1698236070745", "value": [{"ks": "ks1", "cf": "tbl1", "total": 35056, "live": 0},
                                               {"ks": "ks1", "cf": "tbl2", "total": 20956, "live": 0}]},
            ]),
        expected_request("GET", "/storage_service/snapshots/size/true", response=945235),
        ])

    cassandra_expected_output =\
"""Snapshot Details: 
Snapshot name Keyspace name Column family name True size Size on disk
1698236289867 ks1           tbl1               0 bytes   44 KB       
1698236289867 ks1           tbl2               0 bytes   40 KB       
1698236070745 ks1           tbl1               0 bytes   34.23 KB    
1698236070745 ks1           tbl2               0 bytes   20.46 KB    

Total TrueDiskSpaceUsed: 923.08 KiB

"""
    scylla_expected_output =\
"""Snapshot Details:
Snapshot name Keyspace name Column family name True size Size on disk
1698236289867 ks1           tbl1                   0 B         44 KiB
1698236289867 ks1           tbl2                   0 B         40 KiB
1698236070745 ks1           tbl1                   0 B         34 KiB
1698236070745 ks1           tbl2                   0 B         20 KiB

Total TrueDiskSpaceUsed: 923 KiB

"""

    if request.config.getoption("nodetool") == "scylla":
        assert res == scylla_expected_output
    else:
        assert res == cassandra_expected_output
