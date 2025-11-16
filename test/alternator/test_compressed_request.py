# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for handling of compressed *requests*.
#
# According to DynamoDB's documentation,
# https://docs.aws.amazon.com/sdkref/latest/guide/feature-compression.html
# Some SDKs can send compressed requests which the server needs to be able
# to handle - and the handling of these compressed requests is what we intend
# to test in this file.
#
# A "compressed request" has uncompressed HTTP headers (this is unfortunately
# necessary in HTTP), the "Content-Encoding: gzip" header, and the request
# body is a gzip'ed version of the original uncompressed body.
#
# We can write this test with boto3 because boto3 is one of the SDKs listed
# as supporting sending of compressed requests (this feature was implemented
# in 2023, in https://github.com/boto/botocore/pull/2959). But as you'll see
# below *currently* the API for actually enabling request compression in the
# SDK isn't very user-friendly, and will surely change in the future (or we'll
# need to provide our own version of the SDK that does it automatically).
# But the SDK's API isn't the point of this test - the point of this test is
# to check if DynamoDB understands compressed results, and if Alternator does.
#
# Note that the tests here are just for compression of *requests*. The issue
# of compression of *responses* - the client specifies "Accept-Encoding:"
# and the server returning a compressed response - is a separate issue and
# not tested here.

import boto3
import botocore
import pytest

from .util import random_string

# The compressed_req fixture is like the dynamodb fixture - providing a
# connection to a DynamDB-API server. But the unique feature of compressed_req
# is that it automatically compresses (using gzip) every request sent over it.
# NOTE: If your test uses a test-table fixture, don't perform requests using
# that fixture because those requests will use the standard uncompressed
# "dynamodb" connection. Instead, do
#      tab = compressed_req.Table(test_table.name)
# and use the new "tab" object to perform requests.
@pytest.fixture(scope="module")
def compressed_req(dynamodb):
    # Copy URL, most configuration, and credentials from the existing
    # "dynamodb" fixture:
    url = dynamodb.meta.client._endpoint.host
    config = dynamodb.meta.client._client_config
    credentials = dynamodb.meta.client._request_signer._credentials
    verify = not url.startswith('https')
    # By default, the SDK only bothers to compress requests larger than 10KB.
    # Let's drop that limit to 1 byte.
    config = config.merge(botocore.client.Config(request_min_compression_size_bytes=1))
    ret = boto3.resource('dynamodb', endpoint_url=url, verify=verify,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        region_name='us-east-1', config=config)
    # Unfortunately, request compression is currently not enabled by default
    # for DynamoDB requests, and there is no user-visible way to enable it.
    # Instead, compression needs to be chosen by botocore for each individual
    # operation type (e.g., PutItem), through its service-description file
    # botocore/data/dynamodb/2012-08-10/service-2.json. We'll need to override
    # the content of that file by the following trickery, enabling compression
    # for all DynamoDB API operations.
    service_model = ret.meta.client.meta.service_model
    for op in service_model._service_description['operations']:
        op_def = service_model._service_description['operations'][op]
        op_def['requestcompression'] = {'encodings': ['gzip']}
    yield ret
    ret.meta.client.close()

# A basic test for a compressed request, using PutItem and GetItem
@pytest.mark.xfail(reason='issue #5041')
def test_compressed_request(test_table_s, compressed_req):
    tab = compressed_req.Table(test_table_s.name)
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    tab.put_item(Item=item)
    got_item = tab.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert got_item == item
