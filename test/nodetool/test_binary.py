#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request


def test_disablebinary(nodetool):
    nodetool("disablebinary", expected_requests=[
        expected_request("DELETE", "/storage_service/native_transport")])


def test_enablebinary(nodetool):
    nodetool("enablebinary", expected_requests=[
        expected_request("POST", "/storage_service/native_transport")])


def test_statusbinary(nodetool):
    out = nodetool("statusbinary", expected_requests=[
        expected_request("GET", "/storage_service/native_transport", response=False)])
    assert out == "not running\n"

    out = nodetool("statusbinary", expected_requests=[
        expected_request("GET", "/storage_service/native_transport", response=True)])
    assert out == "running\n"
