#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request


def test_disablegossip(nodetool):
    nodetool("disablegossip", expected_requests=[
        expected_request("DELETE", "/storage_service/gossiping")])


def test_enablegossip(nodetool):
    nodetool("enablegossip", expected_requests=[
        expected_request("POST", "/storage_service/gossiping")])


def test_statusgossip(nodetool):
    res = nodetool("statusgossip", expected_requests=[
        expected_request("GET", "/storage_service/gossiping", response=False)])
    assert res.stdout == "not running\n"

    res = nodetool("statusgossip", expected_requests=[
        expected_request("GET", "/storage_service/gossiping", response=True)])
    assert res.stdout == "running\n"
