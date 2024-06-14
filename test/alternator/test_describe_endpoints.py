# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Test for the DescribeEndpoints operation

import boto3
from test.alternator.conftest import get_valid_alternator_role

# Test that the DescribeEndpoints operation works as expected: that it
# returns one endpoint (it may return more, but it never does this in
# Amazon), and this endpoint can be used to make more requests.
def test_describe_endpoints(request, dynamodb):
    endpoints = dynamodb.meta.client.describe_endpoints()['Endpoints']
    # It is not strictly necessary that only a single endpoint be returned,
    # but this is what Amazon DynamoDB does today (and so does Alternator).
    assert len(endpoints) == 1
    for endpoint in endpoints:
        assert 'CachePeriodInMinutes' in endpoint.keys()
        address = endpoint['Address']
        # Check that the address is a valid endpoint by checking that we can
        # send it another describe_endpoints() request ;-) Note that the
        # address does not include the "http://" or "https://" prefix, and
        # we need to choose one manually.
        prefix = "https://" if request.config.getoption('https') else "http://"
        verify = not request.config.getoption('https')
        url = prefix + address
        if address.endswith('.amazonaws.com'):
            boto3.client('dynamodb',endpoint_url=url, verify=verify).describe_endpoints()
        else:
            # Even though we connect to the local installation, Boto3 still
            # requires us to specify dummy region and credential parameters,
            # otherwise the user is forced to properly configure ~/.aws even
            # for local runs.
            user, secret = get_valid_alternator_role(url)
            boto3.client('dynamodb',endpoint_url=url, region_name='us-east-1', aws_access_key_id=user, aws_secret_access_key=secret, verify=verify).describe_endpoints()
        # Nothing to check here - if the above call failed with an exception,
        # the test would fail.
