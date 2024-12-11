#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
import pytest

def test_get_stream_throughput(nodetool, scylla_only):
    res = nodetool("getstreamthroughput", "--mib", expected_requests = [
            expected_request("GET", "/storage_service/stream_throughput", response=100)
        ])
    assert res.stdout == "100\n"

def test_get_stream_throughput_mbits(nodetool, scylla_only):
    res = nodetool("getstreamthroughput", expected_requests = [
            expected_request("GET", "/storage_service/stream_throughput", response=100)
        ])
    assert res.stdout == f"{int(100*1024*1024*8/1000000)}\n"

def test_set_stream_throughput(nodetool, scylla_only):
    nodetool("setstreamthroughput", "100", expected_requests = [
            expected_request("POST", "/storage_service/stream_throughput", params={"value": "100"})
        ])
