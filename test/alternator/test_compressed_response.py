# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for handling of compressed *responses*.
#
# A "compressed response" is similar to requests (see `test_compressed_request.py`)
# - it has uncompressed HTTP headers with the "Content-Encoding" header indicating the
# compression encoding used (e.g., "gzip"), and the response body 
# that is a compressed version of the original uncompressed body.
#
# The client indicates that it can accept compressed responses by sending 
# the "Accept-Encoding" header in the request, listing the compression encodings
# it supports (e.g., "gzip, deflate") potentially with weight values.
# RFC 9110 specifies also special encoding values "*" and "identity".
#
# DynamoDB implements both gzip and deflate compression.
#
# In these tests we verify that when the client sends the "Accept-Encoding" header,
# responses larger than the configured threshold (4KB for DynamoDB) are compressed
# with one of the requested encodings.
# Boto3 does not support automatic decompression of responses, so we implement
# a context manager `client_response_decompression` that registers an event handler
# to decompress responses before they are parsed by Boto3.
# Another event handler (controlled by `request_custom_headers`) 
# is used to add the "Accept-Encoding" header to requests.

import gzip
import zlib
import pytest
from contextlib import contextmanager

from .util import random_string, is_aws
from test.cqlpy.util import config_value_context

# The threshold for response compression in DynamoDB is 4KB.
DDB_RESPONSE_COMPRESSION_THRESHOLD = 4096

# Context manager to register and unregister an event handler
@contextmanager
def register_event_handler(client, event_name, handler):
    client.meta.events.register(event_name, handler)
    try:
        yield
    finally:
        client.meta.events.unregister(event_name, handler)

# Register the event handler to add custom headers
@contextmanager
def request_custom_headers(dynamodb, headers):
    if isinstance(headers, dict):
        # Here we can add header before the request is signed:
        def add_request_custom_headers(params, **kwargs):
            params['headers'].update(headers)
        with register_event_handler(dynamodb.meta.client, 'before-call', add_request_custom_headers):
            yield
    else:
        # If we want to test repeated headers we can provide list of dicts,
        # but this will mess-up signing.
        # For repeated headers, we need to add them after signing.
        def append_request_custom_headers(request, **kwargs):
            for header_set in headers:
                for k, v in header_set.items():
                    request.headers.add_header(k, v)
        with register_event_handler(dynamodb.meta.client, 'request-created', append_request_custom_headers):
            yield

# Enable decompressing responses before parsing
# It also adds some info about compression to the response dict
@contextmanager
def client_response_decompression(dynamodb):
    def decompress_response(response_dict, customized_response_dict, **kwargs):
        customized_response_dict["HTTPContentLength"] = len(response_dict['body'])
        if 'Content-Encoding' in response_dict['headers']:
            encoding = response_dict['headers']['Content-Encoding'].lower()
            if encoding == 'identity':
                return
            customized_response_dict["WasCompressed"] = encoding
            if encoding == 'gzip':
                response_dict['body'] = gzip.decompress(response_dict['body'])
            if encoding == 'deflate':
                response_dict['body'] = zlib.decompress(response_dict['body'])
            customized_response_dict["DecompressedLength"] = len(response_dict['body'])
        
    # Register the decompression handler - this runs before parsing
    with register_event_handler(dynamodb.meta.client, 'before-parse', decompress_response):
        yield

# Context manager to enable/disable Alternator's response compression, by setting response size compression threshold.
# We can't do it for DynamoDB - For DynamoDBs it only skips test on unsupported encoding (if we decide to add them for Alternator).
# Directly using the `cql` fixture makes a test Scylla-only.
# To use with both Alternator/DynamoDB, use `request` fixture instead.
@contextmanager
def server_response_compression(dynamodb, cql_request, encoding, compression_threshold=DDB_RESPONSE_COMPRESSION_THRESHOLD):
    if hasattr(cql_request, 'getfixturevalue'):
        if is_aws(dynamodb):
            if encoding != 'gzip' and encoding != 'deflate':
                pytest.skip(f"DynamoDB only supports gzip and deflate compression, not {encoding}.")
            # For DynamoDB just yield - no server-side configuration needed
            yield
            return
        # Get real cql fixture if not directly provided
        cql_request = cql_request.getfixturevalue('cql')
    with config_value_context(cql_request, f'alternator_response_{encoding}_compression_threshold_in_bytes', str(compression_threshold)):
        yield

# Basic test for compressed responses from the server.
# This test works for both Alternator and DynamoDB.
# In this test, we check that with 'Accept-Encoding' header in the request
# responses above DynamoDBs threshold are compressed with the requested encoding and smaller ones are not.
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_compressed_response(dynamodb, test_table_s, request, encoding):
    p = random_string()
    # single column slightly below threshold, but full row response will be above it
    compressable_data = 'x' * (DDB_RESPONSE_COMPRESSION_THRESHOLD - 25)
    test_table_s.put_item(Item={'p': p, 'x': compressable_data})
                
    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb, request, encoding):

            # First, verify that without the 'Accept-Encoding: gzip' header, we get normal response
            response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='p, x')
            assert response['Item']['x'] == compressable_data
            assert "WasCompressed" not in response,\
                f"Response was '{response['WasCompressed']}' compressed, when it shouldn't be."

            # Now check the compression case
            with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
                response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='p, x')
                assert response['Item']['x'] == compressable_data
                assert "WasCompressed" in response, f"Response was not compressed."
                assert response["WasCompressed"] == encoding, \
                    f"Response was compressed with {response['WasCompressed']}, not {encoding}."
                # Not very precise, but check that compression actually happened
                assert response["DecompressedLength"] > 30 * response["HTTPContentLength"]

                # But also check that responses below threshold are not compressed
                # even if 'Accept-Encoding' header is present.
                response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='x')
                assert response['Item']['x'] == compressable_data
                assert "WasCompressed" not in response,\
                    f"Response was '{response['WasCompressed']}' compressed, when it shouldn't be."

# Similar to `test_compressed_response`, but now we also test with poorly compressable data, 
# which result in larger response and with this test we can see if we can avoid large allocations.
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_compressed_response_large(dynamodb, test_table_s, request, encoding):
    p = random_string()
    large_poorly_compressable_data = random_string(length=DDB_RESPONSE_COMPRESSION_THRESHOLD * 50)
    test_table_s.put_item(Item={'p': p, 'y': large_poorly_compressable_data})
                
    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb, request, encoding):
            with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
                response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='y')
                assert response['Item']['y'] == large_poorly_compressable_data
                assert "WasCompressed" in response, f"Response was not compressed."
                assert response["WasCompressed"] == encoding, \
                    f"Response was compressed with {response['WasCompressed']}, not {encoding}."
                # Not very precise, but check that some compression actually happened
                assert response["DecompressedLength"] > 1.2 * response["HTTPContentLength"]

# Separate test to check that compression works also for chunked responses.
# To trigger chunked responses, we can use the BatchGetItem with response above 100KB.
# DynamoDB does not use chunked responses, so it is scylla only test (by direct use of `cql` fixture)
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_compressed_response_chunked(dynamodb, test_table_s, cql, encoding):
    p = [random_string() for _ in range(10)]

    large_compressable_data = 'x' * 1024 * 150 # 150KB
    large_poorly_compressable_data = random_string(length=1024 * 150)
    with test_table_s.batch_writer() as batch:
        for key in p:
            batch.put_item(Item={'p': key, 'x': large_compressable_data, 'y': large_poorly_compressable_data})

    # The request we will test:
    def get_batch(column):
        response = dynamodb.batch_get_item(RequestItems={
                test_table_s.name: {
                    'Keys': [{'p': key} for key in p],
                    'ProjectionExpression': f"p, {column}"
                }
            }
        )
        v = response['Responses'][test_table_s.name]
        assert len(v) == len(p)
        for item in v:
            assert item[column] == (large_compressable_data if column == 'x' else large_poorly_compressable_data)
        return response
    
    # Make sure that we are testing chunked responses
    def assert_chunked(response_dict, **kwargs):
        assert 'Transfer-Encoding' in response_dict['headers'] \
                    and response_dict['headers']['Transfer-Encoding'] == 'chunked', \
                f"Response is not chunked."
        
    with register_event_handler(dynamodb.meta.client, 'before-parse', assert_chunked):    
        with client_response_decompression(dynamodb):
            # Make sure compression is enabled for Alternator
            with server_response_compression(dynamodb, cql, encoding):
                # First, verify that without the 'Accept-Encoding' header, we get normal response
                response = get_batch('x')
                assert "WasCompressed" not in response, f"Response was '{response['WasCompressed']}' compressed, when it shouldn't be."

                with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
                    response = get_batch('x')
                    assert "WasCompressed" in response, "Response was not compressed."
                    assert response["WasCompressed"] == encoding, f"Response was compressed with {response['WasCompressed']}, not {encoding}."
                    # Not very precise, but check that some compression actually happened
                    assert response["DecompressedLength"] > 30 * response["HTTPContentLength"]

                    response = get_batch('y')
                    assert "WasCompressed" in response, "Response was not compressed."
                    assert response["WasCompressed"] == encoding, f"Response was compressed with {response['WasCompressed']}, not {encoding}."
                    # Not very precise, but check that some compression actually happened
                    assert response["DecompressedLength"] > 1.2 * response["HTTPContentLength"]

# The test checks various values of 'Accept-Encoding' header.
# It works for both Alternator and DynamoDB.
@pytest.mark.xfail(reason='issue #27246')
def test_compressed_response_accept_encodings(dynamodb, test_table_s, request):
    p = random_string()

    compressable_data = 'x' * DDB_RESPONSE_COMPRESSION_THRESHOLD
    test_table_s.put_item(Item={'p': p, 'x': compressable_data})

    # Helper to check expected response compression with given custom headers
    def check_response_with_custom_headers(custom_headers, should_be_compressed):
        with request_custom_headers(dynamodb, custom_headers):
            response = test_table_s.get_item(Key={'p': p})
            assert response['Item']['x'] == compressable_data
            if should_be_compressed is None:
                assert "WasCompressed" not in response,\
                    f"Response to {custom_headers} was '{response['WasCompressed']}' compressed, when it shouldn't be."
            else:
                assert "WasCompressed" in response, f"Response to {custom_headers} was not compressed."
                assert response["WasCompressed"] == should_be_compressed, \
                    f"Response to {custom_headers} was compressed with {response['WasCompressed']}, not {should_be_compressed}."

    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb, request, "gzip"):
            with server_response_compression(dynamodb, request, "deflate"):
                # Check Accept-Encoding values other than pure 'gzip' or 'deflate'
                check_response_with_custom_headers({"Accept-Encoding": "identity"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": ""}, should_be_compressed=None)

                # unsupported encoding - this may change if we implement other encodings
                check_response_with_custom_headers({"Accept-Encoding": "br"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "zstd"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "compress"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "x-gzip"}, should_be_compressed=None)

                # multiple encodings without quality values
                check_response_with_custom_headers({"Accept-Encoding": "gzip, deflate"}, should_be_compressed='gzip')
                check_response_with_custom_headers({"Accept-Encoding": "br, identity, deflate, gzip"}, should_be_compressed='deflate') #order matters
                check_response_with_custom_headers({"Accept-Encoding": "GZIP, identity"}, should_be_compressed='gzip')
                check_response_with_custom_headers({"AcCePt-EnCoDiNg": "dog, gzip ,, cat"}, should_be_compressed='gzip')
                # multiple encodings with quality values
                check_response_with_custom_headers({"Accept-Encoding": "gzip;q=0.8, deflate;q=0.5"}, should_be_compressed='gzip')
                check_response_with_custom_headers({"Accept-Encoding": "br, gzip;q=0, deflate; q = 0.5 , identity;q=0"}, should_be_compressed='deflate')

                # some other tricky cases
                check_response_with_custom_headers({"Accept-Encoding": "*"}, should_be_compressed="gzip") # gzip prefered
                check_response_with_custom_headers({"Accept-Encoding": "*, deflate;q=0.5"}, should_be_compressed='gzip')
                check_response_with_custom_headers({"Accept-Encoding": "*, deflate"}, should_be_compressed='deflate')
                check_response_with_custom_headers({"Accept-Encoding": "*, identity"}, should_be_compressed='gzip')
                check_response_with_custom_headers({"Accept-Encoding": "*, gzip;q=0.5"}, should_be_compressed='deflate')
                check_response_with_custom_headers({"Accept-Encoding": "gzip ;q=0"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "identity;q=0"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "*;q=0"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "*;q=0.5,identity"}, should_be_compressed="gzip") # literally it would mean 'no encoding' preferred, but seems like identity is ignored
                check_response_with_custom_headers({"Accept-Encoding": "gzip;q=0.5,identity;q=0.8"}, should_be_compressed="gzip")
                check_response_with_custom_headers({"Accept-Encoding": "br;q=0.8,identity;q=0.5,*;q=0"}, should_be_compressed=None)
                # while repeating this header is legal, both DynamoDB and seastar seem to only consider the last one.
                # But Alternator returns `wrong signature` error.
                #check_response_with_custom_headers([{"Accept-Encoding": "identity"}, {"Accept-Encoding": "gzip"}], should_be_compressed='gzip')
                #check_response_with_custom_headers([{"Accept-Encoding": "gzip"}, {"Accept-Encoding": "identity"}], should_be_compressed=None)

                # some invalid values, but apparently not always fully rejected by DynamoDB
                check_response_with_custom_headers({"Accept-Encoding": "\"gzip\""}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "gzip br"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "gzip:q=1"}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5, gzip;q=31"}, should_be_compressed="gzip") # although incorrect, quality > 1 treated as 1
                check_response_with_custom_headers({"Accept-Encoding": "gzip;q=31,deflate;q=51"}, should_be_compressed="gzip") # although incorrect, quality > 1 treated as 1
                check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5, gzip;q=0.2"}, should_be_compressed="deflate")
                check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5, gzip;q=1;q=0.2"}, should_be_compressed="gzip") # although incorrect entry, first quality value is used in DynamoDB
                check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5,gzip;q=0."}, should_be_compressed="deflate")
                check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5,gzip;q=0.x"}, should_be_compressed="gzip")
                #Alternator returns `wrong signature` error
                #check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5,gzip;q=0.   x"}, should_be_compressed="gzip")
                check_response_with_custom_headers({"Accept-Encoding": "deflate;q=-0.5,gzip;q=0.2"}, should_be_compressed="gzip")
                check_response_with_custom_headers({"Accept-Encoding": "gzip;nq=1;q=0 "}, should_be_compressed=None)
                check_response_with_custom_headers({"Accept-Encoding": "gzip;q=0, gzip;q=1, gzip;q=0"}, should_be_compressed="gzip")
                check_response_with_custom_headers({"Accept-Encoding": "br,identity;q=0.5,gzip;q=0.0001"}, should_be_compressed="gzip") # although very small quality, still non-zero

# So far we used DynamoDB's default threshold of 4KB for response compression.
# Now test that in Alternator we can set this threshold and enable/disable compressions.
@pytest.mark.xfail(reason='issue #27246')
def test_compressed_response_set_threshold(dynamodb, test_table_s, cql):
    p = random_string()

    compressable_data = 'x' * 40
    test_table_s.put_item(Item={'p': p, 'x': compressable_data})

    # Helper to check expected response compression with given thresholds
    def check_response_with_threshold(gzip_threshold, deflate_threshold, should_be_compressed):
        with server_response_compression(dynamodb, cql, "gzip", compression_threshold=gzip_threshold):
            with server_response_compression(dynamodb, cql, "deflate", compression_threshold=deflate_threshold):
                response = test_table_s.get_item(Key={'p': p})
                assert response['Item']['x'] == compressable_data
                if should_be_compressed is None:
                    assert "WasCompressed" not in response,\
                        f"With thresholds {(gzip_threshold, deflate_threshold)} response was '{response['WasCompressed']}' compressed, when it shouldn't be."
                else:
                    assert "WasCompressed" in response, f"With thresholds {(gzip_threshold, deflate_threshold)} response was not compressed."
                    assert response["WasCompressed"] == should_be_compressed, \
                        f"With thresholds {(gzip_threshold, deflate_threshold)} response was compressed with {response['WasCompressed']}, not {should_be_compressed}."
                return response["HTTPContentLength"]

    with client_response_decompression(dynamodb):
        with request_custom_headers(dynamodb, {"Accept-Encoding": "gzip, deflate"}):
            # With compression disabled no compression and we can get actual response size
            response_length = check_response_with_threshold(gzip_threshold=0, deflate_threshold=0, should_be_compressed=None)

            # Now enable
            check_response_with_threshold(gzip_threshold=response_length, deflate_threshold=0, should_be_compressed="gzip")
            check_response_with_threshold(gzip_threshold=0, deflate_threshold=response_length, should_be_compressed="deflate")

            check_response_with_threshold(gzip_threshold=response_length+1, deflate_threshold=response_length+1, should_be_compressed=None)
            check_response_with_threshold(gzip_threshold=response_length, deflate_threshold=response_length+1, should_be_compressed="gzip")
            check_response_with_threshold(gzip_threshold=response_length+1, deflate_threshold=response_length, should_be_compressed="deflate")
