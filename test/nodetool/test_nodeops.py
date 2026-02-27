#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with


def test_decommission(nodetool):
    nodetool("decommission", expected_requests=[
        expected_request("POST", "/storage_service/decommission")])


def test_rebuild(nodetool):
    nodetool("rebuild", expected_requests=[
        expected_request("POST", "/storage_service/rebuild")])


def test_rebuild_with_dc(nodetool):
    nodetool("rebuild", "DC1", expected_requests=[
        expected_request("POST", "/storage_service/rebuild", params={"source_dc": "DC1"})])


def test_removenode(nodetool):
    nodetool("removenode", "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86", expected_requests=[
        expected_request("POST", "/storage_service/remove_node",
                         params={"host_id": "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86"})])


def test_removenode_ignore_nodes_one_node(nodetool):
    nodetool("removenode",
             "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86",
             "--ignore-dead-nodes",
             "c0f4683f-61aa-43d4-98b5-99e2c5d27952",
             expected_requests=[
                 expected_request("POST", "/storage_service/remove_node", params={
                     "host_id": "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86",
                     "ignore_nodes": "c0f4683f-61aa-43d4-98b5-99e2c5d27952"})])


def test_removenode_ignore_nodes_two_nodes(nodetool):
    nodetool("removenode",
             "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86",
             "--ignore-dead-nodes",
             "c0f4683f-61aa-43d4-98b5-99e2c5d27952,7f066eb5-b76c-4587-922f-d71e2d7c3b51",
             expected_requests=[
                 expected_request("POST", "/storage_service/remove_node", params={
                     "host_id": "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86",
                     "ignore_nodes": "c0f4683f-61aa-43d4-98b5-99e2c5d27952,7f066eb5-b76c-4587-922f-d71e2d7c3b51"})])


def test_removenode_status(nodetool):
    res = nodetool("removenode", "status", expected_requests=[
        expected_request("GET", "/storage_service/removal_status", response="SOME STATUS")])
    assert res.stdout == "RemovalStatus: SOME STATUS\n"


def test_removenode_force(nodetool):
    res = nodetool("removenode", "force", expected_requests=[
        expected_request("GET", "/storage_service/removal_status", response="SOME STATUS"),
        expected_request("POST", "/storage_service/force_remove_completion")])
    assert res.stdout == "RemovalStatus: SOME STATUS\n"


def test_removenode_status_with_ignore_dead_nodes(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("removenode", "status", "--ignore-dead-nodes", "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86"),
            {"expected_requests": []},
            ["error processing arguments: cannot use --ignore-dead-nodes with status or force"])


def test_removenode_force_with_ignore_dead_nodes(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("removenode", "force", "--ignore-dead-nodes", "ac9e2ad5-c6d7-4769-a64b-6e73173ccd86"),
            {"expected_requests": []},
            ["error processing arguments: cannot use --ignore-dead-nodes with status or force"])
