#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request


def test_disablegossip(nodetool):
    nodetool("disablegossip", expected_requests=[
        expected_request("DELETE", "/storage_service/gossiping")])


def test_enablegossip(nodetool):
    nodetool("enablegossip", expected_requests=[
        expected_request("POST", "/storage_service/gossiping")])


def test_statusgossip(nodetool):
    out = nodetool("statusgossip", expected_requests=[
        expected_request("GET", "/storage_service/gossiping", response=False)])
    assert out == "not running\n"

    out = nodetool("statusgossip", expected_requests=[
        expected_request("GET", "/storage_service/gossiping", response=True)])
    assert out == "running\n"
