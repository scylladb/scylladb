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
