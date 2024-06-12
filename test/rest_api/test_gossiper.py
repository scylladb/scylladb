# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import sys
import requests
import threading
import time

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')

def test_gossiper_live_endpoints(cql, rest_api):
    resp = rest_api.send("GET", f"gossiper/endpoint/live")
    resp.raise_for_status()
    live_endpoints = set(resp.json())
    all_hosts_endpoints = set([host.address for host in cql.cluster.metadata.all_hosts()])
    assert live_endpoints == all_hosts_endpoints

def test_gossiper_unreachable_endpoints(cql, rest_api):
    resp = rest_api.send("GET", f"gossiper/endpoint/down")
    resp.raise_for_status()
    unreachable_endpoints = set(resp.json())
    assert not unreachable_endpoints

def test_gossiper_unreachable_endpoints(cql, rest_api):
    resp = rest_api.send("GET", f"gossiper/endpoint/down")
    resp.raise_for_status()
    unreachable_endpoints = set(resp.json())
    for ep in unreachable_endpoints:
        resp = rest_api.send("GET", f"gossiper/downtime/{ep}")
        resp.raise_for_status()
        assert int(resp.json()) > 0

    resp = rest_api.send("GET", f"gossiper/endpoint/live")
    resp.raise_for_status()
    live_endpoints = set(resp.json())
    for ep in live_endpoints:
        resp = rest_api.send("GET", f"gossiper/downtime/{ep}")
        resp.raise_for_status()
        assert int(resp.json()) == 0
