# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for CRC response header (x-amz-crc32) support.
# DynamoDB always returns this header, but most SDKs do not check it, so by
# default Alternator doesn't bother to calculate and return it - as it's a
# waste of performance. However, if a customer for some reason requires this
# CRC for compatibility, this option can be enabled, and the goal of this test
# file is to check that it is correctly calculated, just like in DynamoDB.

import pytest
import binascii
import json
import requests
from contextlib import contextmanager
from .util import get_signed_request, is_aws, scylla_config_temporary, random_string

# Make a manual request, similar to manual_request() from util.py but without
# parsing the response at all. Returns the unparsed response object, from which
# you can read response.headers, response.content and response.status_code.
# This function asks the server to NOT compress responses, by sending
# "Accept-Encoding: identity" in the request.
def unparsed_manual_request_uncompressed(dynamodb, op, payload):
    req = get_signed_request(dynamodb, op, payload,
                             extra_headers={'Accept-Encoding': 'identity'})
    res = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    return res

# Make a manual request, similar to unparsed_manual_request_uncompressed() but
# asks the server to compress the response (using Accept-Encoding: gzip) and
# returns both the response object and the raw compressed body bytes as a tuple
# (response, body), without decompressing the body.
# Note that this function does not force the server to compress the response -
# it may still decide to not compress it, e.g., based on the response length.
def unparsed_manual_request_compressed(dynamodb, op, payload):
    req = get_signed_request(dynamodb, op, payload,
                             extra_headers={'Accept-Encoding': 'gzip'})
    response = requests.post(req.url, headers=req.headers, data=req.body,
                             verify=False, stream=True)
    response.raw.decode_content = False
    try:
        body = response.raw.read()
    finally:
        response.close()
    return response, body

# Context manager to temporarily enable the x-amz-crc32 header in responses
# in Alternator (since it's off by default). For DynamoDB, this is a no-op
# because DynamoDB always sends this header.
@contextmanager
def crc32_header_enabled(dynamodb):
    with scylla_config_temporary(dynamodb, 'alternator_response_crc32_header', 'true', nop=is_aws(dynamodb)):
        yield

# Check that the x-amz-crc32 header value matches the CRC32 of the response
# body.
def check_crc32(headers, body):
    assert 'x-amz-crc32' in headers
    header_crc = int(headers['x-amz-crc32'])
    response_crc = binascii.crc32(body)
    assert response_crc == header_crc

# A simple test that the x-amz-crc32 header is present in a response of a
# simple request and has the correct value
def test_crc32_header(dynamodb, test_table_s):
    p = random_string()
    test_table_s.put_item(Item={'p': p, 'x': 'hello'})
    with crc32_header_enabled(dynamodb):
        response = unparsed_manual_request_uncompressed(dynamodb, 'GetItem',
            json.dumps({'TableName': test_table_s.name,
                        'Key': {'p': {'S': p}},
                        'ConsistentRead': True}))
    assert response.status_code == 200
    check_crc32(response.headers, response.content)

# On Alternator, by default the x-amz-crc32 header should not be returned,
# because alternator_response_crc32_header is disabled by default.
# This test is scylla_only (skipped on DynamoDB), because in DynamoDB there
# is no way to disable the header.
def test_crc32_header_disabled_by_default(dynamodb, test_table_s, scylla_only):
    p = random_string()
    test_table_s.put_item(Item={'p': p})
    response = unparsed_manual_request_uncompressed(dynamodb, 'GetItem',
        json.dumps({'TableName': test_table_s.name, 'Key': {'p': {'S': p}}}))
    assert response.status_code == 200
    assert 'x-amz-crc32' not in response.headers

# Test that the CRC is also correct on error responses.
def test_crc32_header_on_error(dynamodb):
    with crc32_header_enabled(dynamodb):
        # Request a non-existent table to trigger a ResourceNotFoundException
        response = unparsed_manual_request_uncompressed(dynamodb, 'GetItem',
            json.dumps({'TableName': 'nonexistent_table_xyz',
                        'Key': {'p': {'S': 'x'}}}))
    assert response.status_code != 200
    check_crc32(response.headers, response.content)

# Test that when the response is compressed, the CRC header is calculated over
# the *compressed* data - not for the uncompressed response body.
def test_crc32_header_with_compression(dynamodb, test_table_s):
    p = random_string()
    # Large enough to exceed DynamoDB/Alternator's compression threshold (~4 KB)
    large_value = 'x' * 5000
    test_table_s.put_item(Item={'p': p, 'data': large_value})
    with crc32_header_enabled(dynamodb):
        # On Alternator, we can set the response compression threshold to also
        # be 4KB, which is already the default on DynamoDB.
        with scylla_config_temporary(dynamodb, f'alternator_response_compression_threshold_in_bytes', '4096', nop=is_aws(dynamodb)):
            response, body = unparsed_manual_request_compressed(dynamodb, 'GetItem',
                json.dumps({'TableName': test_table_s.name,
                            'Key': {'p': {'S': p}},
                            'ConsistentRead': True}))
    # Verify the response body was actually compressed, otherwise this test
    # doesn't exercise the compression code path.
    assert response.headers.get('Content-Encoding', '').lower() == 'gzip'
    assert body[:2] == b'\x1f\x8b'  # gzip magic bytes
    # Finally verify the CRC header is correct for the compressed body
    check_crc32(response.headers, body)

# For large BatchGetItem responses (among other things), Alternator uses a
# different code path that streams the response, and will need to collect it
# all (and compress it all, if compression is active) to calculate the CRC
# before sending the first byte of the response. So this test exercises this
# case.
def test_crc32_header_with_streaming(dynamodb, test_table_s):
    p = random_string()
    large_value = 'x' * 100001
    test_table_s.put_item(Item={'p': p, 'data': large_value})
    with crc32_header_enabled(dynamodb):
        response = unparsed_manual_request_uncompressed(dynamodb, 'BatchGetItem',
            json.dumps({'RequestItems': {test_table_s.name: {
                'Keys': [{'p': {'S': p}}],
                'ConsistentRead': True}}}))
    assert response.status_code == 200
    check_crc32(response.headers, response.content)

def test_crc32_header_with_streaming_and_compression(dynamodb, test_table_s):
    p = random_string()
    large_value = 'x' * 100001
    test_table_s.put_item(Item={'p': p, 'data': large_value})
    with crc32_header_enabled(dynamodb):
        with scylla_config_temporary(dynamodb, 'alternator_response_compression_threshold_in_bytes', '4096', nop=is_aws(dynamodb)):
            response, body = unparsed_manual_request_compressed(dynamodb, 'BatchGetItem',
                json.dumps({'RequestItems': {test_table_s.name: {
                    'Keys': [{'p': {'S': p}}],
                    'ConsistentRead': True}}}))
    # Verify the response body was actually compressed, otherwise this test
    # doesn't exercise the compression code path.
    assert response.headers.get('Content-Encoding', '').lower() == 'gzip'
    assert body[:2] == b'\x1f\x8b'  # gzip magic bytes
    # Finally verify the CRC header is correct for the compressed body
    check_crc32(response.headers, body)
