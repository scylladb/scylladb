# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import pytest
import requests
import time

def get_uptime_ms(rest_api):
    resp = rest_api.send('GET', "system/uptime_ms")
    resp.raise_for_status()
    return int(resp.text)

# Check that system/uptime_ms advances along with the real time: the
# difference between two readings must be within the time measured around
# the two requests (with a small slack for rounding to milliseconds).
def test_system_uptime_ms(rest_api):
    t0 = time.monotonic()
    uptime1 = get_uptime_ms(rest_api)
    t1 = time.monotonic()
    time.sleep(1)
    t2 = time.monotonic()
    uptime2 = get_uptime_ms(rest_api)
    t3 = time.monotonic()
    assert (t2 - t1) * 1000 - 2 <= uptime2 - uptime1 <= (t3 - t0) * 1000 + 2


def test_system_highest_sstable_format(rest_api):
    resp = rest_api.send('GET', "system/highest_supported_sstable_version")
    resp.raise_for_status()
    assert resp.json() == "mt"

def test_chosen_sstable_format(rest_api):
    resp = rest_api.send('GET', "system/chosen_sstable_version")
    resp.raise_for_status()
    assert resp.json() == "mt"

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
