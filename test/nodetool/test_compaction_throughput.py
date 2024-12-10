#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
import pytest

def test_get_compaction_throughput(nodetool, scylla_only):
    res = nodetool("getcompactionthroughput", expected_requests = [
            expected_request("GET", "/storage_service/compaction_throughput", response=0)
        ])
    assert res.stdout == '0\n'

def test_set_compaction_throughput(nodetool, scylla_only):
    nodetool("setcompactionthroughput", "100", expected_requests = [
            expected_request("POST", "/storage_service/compaction_throughput", params={"value": "100"})
        ])
