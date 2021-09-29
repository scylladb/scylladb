# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

##############################################################################
# Tests for Scylla's metrics (see docs/design-notes/metrics.md) for Alternator
# queries. Reproduces issue #9406, where although metrics was implemented for
# Alternator requests, they were missing for some operations (BatchGetItem).
# In the tests here we attempt to ensure that the metrics continue to work
# for the relevant operations as the code evolves.
#
# Note that all tests in this file test Scylla-specific features, and are
# "skipped" when not running against Scylla, or when unable to retrieve
# metrics through out-of-band HTTP requests to Scylla's Prometheus port (9180).
#
# IMPORTANT: we do not want these tests to assume that are not running in
# parallel with any other tests or workload - because such an assumption
# would limit our test deployment options in the future. NOT making this
# assumption means that these tests can't check that a certain operation
# increases a certain counter by exactly 1 - because other concurrent
# operations might increase it further! So our test can only check that the
# counter increases.
##############################################################################

import pytest
import requests
import re

from util import random_string

# Fixture for checking if we are able to test Scylla metrics. Scylla metrics
# are not available on AWS (of course), but may also not be available for
# Scylla if for some reason we have only access to the Alternator protocol
# port but no access to the metrics port (9180).
# If metrics are *not* available, tests using this fixture will be skipped.
# Tests using this fixture may call get_metrics(metrics).
@pytest.fixture(scope="module")
def metrics(dynamodb):
    if dynamodb.meta.client._endpoint.host.endswith('.amazonaws.com'):
        pytest.skip('Scylla-only feature not supported by AWS')
    url = dynamodb.meta.client._endpoint.host
    # The Prometheus API is on port 9180, and always http, not https.
    url = re.sub(r':[0-9]+(/|$)', ':9180', url)
    url = re.sub(r'^https:', 'http:', url)
    url = url + '/metrics'
    resp = requests.get(url)
    if resp.status_code != 200:
        pytest.skip('Metrics port 9180 is not available')
    yield url

# Utility function for fetching all metrics from Scylla, using an HTTP request
# to port 9180. The response format is defined by the Prometheus protocol.
# Only use get_metrics() in a test using the metrics_available fixture.
def get_metrics(metrics):
    response = requests.get(metrics)
    assert response.status_code == 200
    return response.text

# Utility function for fetching a metric with a given name and optionally a
# given sub-metric label (which should be a name-value map). If multiple
# matches are found, they are summed - this is useful for summing up the
# counts from multiple shards.
def get_metric(metrics, name, requested_labels=None):
    total = 0.0
    lines = re.compile('^'+name+'{.*$', re.MULTILINE)
    for match in re.findall(lines, get_metrics(metrics)):
        a = match.split()
        metric = a[0]
        val = float(a[1])
        # Check if match also matches the requested labels
        if requested_labels:
            # we know metric begins with name{ and ends with } - the labels
            # are what we have between those
            got_labels = metric[len(name)+1:-1].split(',')
            # Check that every one of the requested labels is in got_labels:
            for k, v in requested_labels.items():
                if not f'{k}="{v}"' in got_labels:
                    # No match for requested label, skip this metric (python
                    # doesn't have "continue 2" so let's just set val to 0...
                    val = 0
                    break
        total += float(val)
    return total

def test_batch_write_item(test_table_s, metrics):
    n1 = get_metric(metrics, 'scylla_alternator_operation', {'op': 'BatchWriteItem'})
    test_table_s.meta.client.batch_write_item(RequestItems = {
        test_table_s.name: [{'PutRequest': {'Item': {'p': random_string(), 'a': 'hi'}}}]})
    n2 = get_metric(metrics, 'scylla_alternator_operation', {'op': 'BatchWriteItem'})
    assert n2 > n1

# Reproduces issue #9406:
def test_batch_get_item(test_table_s, metrics):
    n1 = get_metric(metrics, 'scylla_alternator_operation', {'op': 'BatchGetItem'})
    test_table_s.meta.client.batch_get_item(RequestItems = {
        test_table_s.name: {'Keys': [{'p': random_string()}], 'ConsistentRead': True}})
    n2 = get_metric(metrics, 'scylla_alternator_operation', {'op': 'BatchGetItem'})
    assert n2 > n1

# TODO: check the rest of the operations
