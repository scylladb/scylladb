# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

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

# The "this_dc" fixture figures out the name of Scylla DC to which "dynamodb"
# is connected. Any test using this fixture automatically becomes scylla_only.
@pytest.fixture(scope="session")
def this_dc(dynamodb, scylla_only):
    tbl = dynamodb.Table('.scylla.alternator.system.local')
    dc = tbl.scan(AttributesToGet=['data_center'])['Items'][0]['data_center']
    yield dc

# The "this_rack" fixture figures out the name of Scylla rack to which "dynamodb"
# is connected. Any test using this fixture automatically becomes scylla_only.
@pytest.fixture(scope="session")
def this_rack(dynamodb, scylla_only):
    tbl = dynamodb.Table('.scylla.alternator.system.local')
    rack = tbl.scan(AttributesToGet=['rack'])['Items'][0]['rack']
    yield rack

# Minimal test for the "dc" option in /localnodes request. In this test framework
# we can't test multiple data centers, but we can test that dc={this_dc} returns
# a node, while dc=nonexistent_dc doesn't.
def test_localnodes_option_dc(scylla_only, dynamodb, this_dc):
    url = dynamodb.meta.client._endpoint.host
    # Using dc={this_dc} should work and return at least this node:
    response = requests.get(url + f'/localnodes?dc={this_dc}', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) >= 1
    # Using dc=nonexistent_dc should return an empty list (not an error)
    response = requests.get(url + f'/localnodes?dc=nonexistent_dc', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) == 0

# Minimal test for the "rack" option in /localnodes request. In this test framework
# we can't test multiple racks, but we can test that rack={this_rack} returns a node,
# and rack=nonexistent_rack doesn't.
def test_localnodes_option_rack(scylla_only, dynamodb, this_rack):
    url = dynamodb.meta.client._endpoint.host
    # Using rack={this_rack} should work and return at least this node:
    response = requests.get(url + f'/localnodes?rack={this_rack}', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) >= 1
    # Using rack=nonexistent_rack should return an empty list (not an error)
    response = requests.get(url + f'/localnodes?rack=nonexistent_rack', verify=False)
    assert response.ok
    j = json.loads(response.content.decode('utf-8'))
    assert isinstance(j, list)
    assert len(j) == 0
