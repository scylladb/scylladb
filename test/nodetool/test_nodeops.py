#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request


def test_decommission(nodetool):
    nodetool("decommission", expected_requests=[
        expected_request("POST", "/storage_service/decommission")])


def test_rebuild(nodetool):
    nodetool("rebuild", expected_requests=[
        expected_request("POST", "/storage_service/rebuild")])


def test_rebuild_with_dc(nodetool):
    nodetool("rebuild", "DC1", expected_requests=[
        expected_request("POST", "/storage_service/rebuild", params={"source_dc": "DC1"})])
