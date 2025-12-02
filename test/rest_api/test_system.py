# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
import requests

def test_system_uptime_ms(rest_api):
    resp = rest_api.send('GET', "system/uptime_ms")
    resp.raise_for_status()


def test_system_highest_sstable_format(rest_api):
    resp = rest_api.send('GET', "system/highest_supported_sstable_version")
    resp.raise_for_status()
    assert resp.json() == "me"

@pytest.mark.parametrize("params", [
    ("storage_service/compaction_throughput", "value"),
    ("storage_service/stream_throughput", "value")
])
def test_io_throughput(rest_api, params):
    resp = rest_api.send("POST", params[0], params={ params[1]: 100 })
    resp.raise_for_status()
    resp = rest_api.send("GET", params[0])
    resp.raise_for_status()
    assert resp.json() == 100
    resp = rest_api.send("POST", params[0], params={ params[1]: 0 })
    resp.raise_for_status()
    resp = rest_api.send("GET", params[0])
    resp.raise_for_status()
    assert resp.json() == 0
    resp = rest_api.send("POST", params[0])
    assert resp.status_code == requests.codes.bad_request
