#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest

from test.nodetool.rest_api_mock import expected_request


def test_rebuild(nodetool):
    nodetool("rebuild", expected_requests=[
        expected_request(
            "POST",
            "/storage_service/rebuild",
            params = {},
            response = "SUCCESSFUL")
    ])


def test_rebuild_source_dc(nodetool):
    source_dc = "UNKNOWN_DC"
    nodetool("rebuild", source_dc, expected_requests=[
        expected_request(
            "POST",
            "/storage_service/rebuild",
            params = {"source_dc": source_dc},
            response = "SUCCESSFUL")
    ])


def test_rebuild_force_source_dc(nodetool, scylla_only):
    source_dc = "UNKNOWN_DC"
    nodetool("rebuild", "--force", source_dc, expected_requests=[
        expected_request(
            "POST",
            "/storage_service/rebuild",
            params = {
                "force": "true",
                "source_dc": source_dc
            },
            response = "SUCCESSFUL")
    ])


def test_rebuild_force_no_source_dc(nodetool):
    nodetool("rebuild", "--force", expected_requests=[
        expected_request(
            "POST",
            "/storage_service/rebuild",
            params = {
                "force": "true"
            },
            response = None,
            response_status = 500)
    ])
