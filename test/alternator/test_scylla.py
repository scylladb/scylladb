# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file contains tests which check Scylla-specific features that do
# not exist on AWS. So all these tests are skipped when running with "--aws".

import pytest
import requests
import json
import urllib.parse

# Test that the "/localnodes" request works, returning at least the one node.
# See more elaborate tests for /localnodes, requiring multiple nodes,
# datacenters, or different configurations, in
# test_topology_experimental_raft/test_alternator.py
def test_localnodes(scylla_only, dynamodb):
    url = dynamodb.meta.client._endpoint.host
    response = requests.get(url + '/localnodes', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) >= 1
    # The host we used to reach Alternator ("url") should normally be one
    # of the nodes returned by /localnodes. In theory, they may be different
    # addresses or aliases of the same node, but in practice, in all ways
    # we normally run this test, it will be the same address.
    # We have a separate test, test_localnodes_broadcast_rpc_address, on how
    # this changes if "broadcast_rpc_address" is configured.
    assert urllib.parse.urlparse(url).hostname in j
