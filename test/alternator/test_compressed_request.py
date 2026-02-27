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
import gzip
import zlib
import requests
import pytest

from .util import random_string
from .test_manual_requests import get_signed_request

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
    region_name = dynamodb.meta.client.meta.region_name
    # By default, the SDK only bothers to compress requests larger than 10KB.
    # Let's drop that limit to 1 byte.
    config = config.merge(botocore.client.Config(request_min_compression_size_bytes=1))
    ret = boto3.resource('dynamodb', endpoint_url=url, verify=verify,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        region_name=region_name, config=config)
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
def test_compressed_request(test_table_s, compressed_req):
    tab = compressed_req.Table(test_table_s.name)
    p = random_string()
    x = random_string()
    item = {'p': p, 'x': x}
    tab.put_item(Item=item)
    got_item = tab.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert got_item == item

# Test a longer PutItem request. Our decompression implementation wants to
# decompress it in pieces, to avoid one long contiguous allocation of the
# output. So this test will check this code path.
def test_long_compressed_request(test_table_s, compressed_req):
    tab = compressed_req.Table(test_table_s.name)
    p = random_string()
    x = random_string()
    # First, make the request compress very well so the compressed request
    # will be very short, but the uncompressed output is long and may
    # be split across multiple output buffers.
    long = p + 'x'*10000
    item = {'p': p, 'x': x, 'long': long}
    tab.put_item(Item=item)
    got_item = tab.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert got_item == item
    # Now try a request that doesn't compress as well. Our implementation
    # may need to split both input and output buffer boundries.
    long = random_string(5000)*2
    item = {'p': p, 'x': x, 'long': long}
    tab.put_item(Item=item)
    got_item = tab.get_item(Key={'p': p}, ConsistentRead=True)['Item']
    assert got_item == item

# The tests above configured boto3 to compress its requests so we could
# test them. We now want to test unusual scenarios - including corrupt
# compressed requests that should fail, or requests compressed in a non-
# traditional way but still should work. We also want to test different
# compression algorithms. We can't test these scenarios using boto3, and
# need to construct the requests on our own using functions from
# test_manual_requests.py.

# Test the error when we send an unsupported Content-Encoding header.
# At the time of this writing, the only supported Content-Encoding are
# "gzip" and "deflate" - the name "garbage" obviously isn't one of them.
def test_garbage_content_encoding(dynamodb, test_table):
    p = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Add a bad Content-Encoding header. The request signature is still valid,
    # but the server will not know how to decode the request.
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'garbage'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500
    # Check the PutItem request really wasn't done
    assert 'Item' not in test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)

# If Content-Encoding is "gzip" but the content is *not* valid gzip encoded,
# DynamoDB returns an InternalServerError. I'm not sure this is the most
# appropriate error to return (among other things it suggests that the
# broken request is retryable), but this is what DynamoDB does so Alternator
# should too.
def test_broken_gzip_content(dynamodb, test_table):
    p = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Add a Content-Encoding header suggesting this is gzipped content.
    # Of course it isn't - it's a valid uncompressed request. The server
    # should fail decompressing it and return a 500 error
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500
    # Check the PutItem request really wasn't done
    assert 'Item' not in test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)

# Test a valid gziped request created manually (without boto3's help). The
# fact that this test passes a sanity test preparing us for the next tests
# where we change the compressed stream and see what happens.
def test_gzip_request_valid(dynamodb, test_table):
    p = random_string()
    v = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}, "v": {"S": "' + v + '"}}}'
    # Compress the payload. The new "payload" will be bytes instead of a
    # string - this is perfectly fine.
    payload = gzip.compress(payload.encode('utf-8'))
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Need to tell the server with a Content-Encoding header that the
    # payload is compressed:
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 200
    got = test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)['Item']
    assert got == {'p': p, 'c': 'x', 'v': v}

# Same test as test_gzip_request_valid() but compress the payload in two
# pieces, concatenating the two gzip outputs. This isn't something users
# will typically do, but is allowed according to the gzip standard so we
# want to support it.
def test_gzip_request_two_gzips(dynamodb, test_table):
    p = random_string()
    v = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}, "v": {"S": "' + v + '"}}}'
    # Compress the payload in two halves - first compress the first 10
    # characters, then the rest, and concatenate the two resulting gzips.
    payload = gzip.compress(payload[:10].encode('utf-8')) + gzip.compress(payload[10:].encode('utf-8'))
    req = get_signed_request(dynamodb, 'PutItem', payload)
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 200
    got = test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)['Item']
    assert got == {'p': p, 'c': 'x', 'v': v}

# Same test as test_gzip_request_valid() but add extra junk - not another
# valid gzip - following the valid gzip string. This should be an error,
# the extra junk should not be just silently ignored.
# Strangely, although we see in other tests for bad gzip that DynamoDB
# returns an error, in this specific case it doesn't. I consider this a
# bug so this is one of the rare tests with the dynamodb_bug marker.
def test_gzip_request_with_extra_junk(dynamodb, test_table, dynamodb_bug):
    p = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}}}'
    payload = gzip.compress(payload.encode('utf-8'))
    # Add junk - which isn't a second valid gzip - at the end of the payload
    payload += b'junk'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500
    assert 'Item' not in test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)

# Same test as test_gzip_request_valid() but remove one character from the
# end of compressed payload, so it is missing the proper ending marker.
# Decompression should fail and generate an error.
def test_gzip_request_with_missing_character(dynamodb, test_table):
    p = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}}}'
    payload = gzip.compress(payload.encode('utf-8'))
    # Remove the one last character from the compressed payload
    payload = payload[:-1]
    req = get_signed_request(dynamodb, 'PutItem', payload)
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500
    assert 'Item' not in test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)

# We put a limit (request_content_length_limit = 16 MB) on the size of the
# request. If the request is compressed, even if the compressed request
# is tiny we should still limit the size of the uncompressed request.
# Let's try a 20 MB that compresses extremely well to a tiny string, but
# should still be rejected as an oversized request.
#
# This test is marked scylla_only because DynamoDB does NOT limit the
# uncompressed size of a compressed request:
# DynamoDB does (as we check in test_manual_requests.py) limit non-compressed
# request sizes, but these limits are not enforced for compressed requests -
# I checked that even a 1 GB (!) pre-compression-size request is accepted
# (but a 10 GB pre-compression-size request is silently rejected with 500
# error). It is debateable whether this should be considered a DynamoDB bug -
# or a "feature". I would claim it's a bug - there is no reason why a huge
# well-compressible request should be allowed while not allowed when not
# compressed. A counter-argument is that the biggest reason why a large
# request is not allowed is the signature algorithm - we need to read
# the entire request to verify its validity and only then we can start acting
# on it. But with a compressed request we validate the small compressed
# version's validity, so we can then - at least in theory - read the
# uncompressed request in a streaming fashion, without ever reading the
# entire request into memory.
def test_gzip_request_oversized(dynamodb, test_table, scylla_only):
    # Take a legal PutItem payload and add a lot of spaces to make it very
    # long, but it's highly compressible so the compressed payload will be
    # very small. The server should still reject the oversized uncompressed
    # content.
    long_len = 20*1024*1024
    p = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}}}'
    payload = payload[:-1] + ' '*long_len + payload[-1]
    payload = gzip.compress(payload.encode('utf-8'))
    # the compressed payload is very small, it won't cause the 413 error
    assert len(payload) < 16*1024*1024
    req = get_signed_request(dynamodb, 'PutItem', payload)
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 413
    assert 'Item' not in test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)

# An empty string is NOT a valid gzip, so if we try to pass it off as a
# a gzip'ed request, the result should be a 500 error like all other
# invalid gzip content.
def test_gzip_request_empty(dynamodb):
    # pass the empty string '' as a (incorrect) compressed payload
    req = get_signed_request(dynamodb, 'PutItem', '')
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'gzip'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500

# After testing requests compressed with "Content-Encoding: gzip", let's
# test support for "deflate" encoding. Deflate is very similar to gzip,
# with a different header. As RFC 9110 explains:
#   The "deflate" coding is a "zlib" data format (RFC 1950) containing a
#   "deflate" compressed data stream (RFC 1951).
def test_deflate_request_valid(dynamodb, test_table):
    p = random_string()
    v = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}, "v": {"S": "' + v + '"}}}'
    payload = zlib.compress(payload.encode('utf-8'))
    req = get_signed_request(dynamodb, 'PutItem', payload)
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'deflate'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 200, f'Request failed: {r.content}'
    got = test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)['Item']
    assert got == {'p': p, 'c': 'x', 'v': v}

# We tested above (test_gzip_request_empty) that an an empty string is not a
# valid gzip. It's not a valid deflate either - if we try to pass it off as a
# a deflated'ed request, the result should be a 500 error like all other
# invalid deflate content.
def test_deflate_request_empty(dynamodb):
    # pass the empty string '' as a (incorrect) compressed payload
    req = get_signed_request(dynamodb, 'PutItem', '')
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'deflate'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500

# Like test_broken_gzip_content also when the content is not a valid deflate
# encoded output, DynamoDB returns InternalServerError. I'm not sure this is
# the most appropriate error to return (among other things it suggests that
# the broken request is retryable), but this is what DynamoDB does so
# Alternator should too.
def test_deflate_request_not_deflated(dynamodb, test_table):
    p = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Add a Content-Encoding header suggesting this is deflate content.
    # Of course it isn't - it's an uncompressed request.
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'deflate'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 500
    # Check the PutItem request really wasn't done
    assert 'Item' not in test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)

# In test_gzip_request_two_gzips() above, we checked that gzip'ing the payload
# in two pieces, concatenating the two gzip outputs, works. This isn't
# something users will typically do, but is explicitly allowed according to
# the gzip standard so both Alternator and DynamoDB allow it. Conversely, for
# "deflate" compression, it is not explicitly specified in RFC 1950 that more
# than one compressed stream can be concatenated. In Alternator we decided to
# allow it - and this test verifies this - but DynamoDB doesn't so this
# test fails there.
def test_deflate_request_two_deflates(dynamodb, test_table, scylla_only):
    p = random_string()
    v = random_string()
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "' + p + '"}, "c": {"S": "x"}, "v": {"S": "' + v + '"}}}'
    # Compress the payload in two halves - first compress the first 10
    # characters, then the rest, and concatenate the two resulting deflates.
    payload = zlib.compress(payload[:10].encode('utf-8')) + zlib.compress(payload[10:].encode('utf-8'))
    req = get_signed_request(dynamodb, 'PutItem', payload)
    headers = dict(req.headers)
    headers.update({'Content-Encoding': 'deflate'})
    r = requests.post(req.url, headers=headers, data=req.body, verify=False)
    assert r.status_code == 200
    got = test_table.get_item(Key={'p': p, 'c': 'x'}, ConsistentRead=True)['Item']
    assert got == {'p': p, 'c': 'x', 'v': v}
