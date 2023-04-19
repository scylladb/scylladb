# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import re

def test_failure_detector_endpoints(rest_api):
    resp = rest_api.send('GET', "failure_detector/endpoints")
    resp.raise_for_status()
    assert len(resp.json()) == 1
    addr = resp.json()[0]['addrs']
    assert re.match(r'\d+\.\d+\.\d+\.\d+', addr)

def test_failure_detector_endpoint_phi_values(rest_api):
    # This api currently always returns empty results
    # so just check it doesn't return an error or crash
    resp = rest_api.send('GET', "failure_detector/endpoint_phi_values")
    resp.raise_for_status()
