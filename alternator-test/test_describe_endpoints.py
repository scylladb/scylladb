# Test for the DescribeEndpoints operation

import boto3

# Test that the DescribeEndpoints operation works as expected: that it
# returns one endpoint (it may return more, but it never does this in
# Amazon), and this endpoint can be used to make more requests.
def test_describe_endpoints(dynamodb):
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
        url = "http://" + address
        boto3.client('dynamodb',endpoint_url=url).describe_endpoints()
        # Nothing to check here - if the above call failed with an exception,
        # the test would fail.
