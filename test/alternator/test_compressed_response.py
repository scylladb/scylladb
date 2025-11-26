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
from test.alternator.util import scylla_config_temporary
import urllib3
from botocore.exceptions import ClientError

# The threshold for response compression in DynamoDB is 4KB.
DDB_RESPONSE_COMPRESSION_THRESHOLD = 4096

# Context manager to register and unregister an event handler
@contextmanager
def register_event_handler(dynamodb, event_name, handler):
    dynamodb.meta.client.meta.events.register(event_name, handler)
    try:
        yield
    finally:
        dynamodb.meta.client.meta.events.unregister(event_name, handler)

# Register the event handler to add custom headers before the request is signed
@contextmanager
def request_custom_headers(dynamodb, headers : dict[str, str]):
    def add_request_custom_headers(params, **kwargs):
        params['headers'].update(headers)
    with register_event_handler(dynamodb, 'before-call', add_request_custom_headers):
        yield

# Enable decompressing responses before parsing
# It also adds some info about compression to the response dict
@contextmanager
def client_response_decompression(dynamodb):
    def decompress_response(response_dict, customized_response_dict, **kwargs):
        customized_response_dict["HTTPContentLength"] = len(response_dict['body'])
        if 'Content-Encoding' in response_dict['headers']:
            encoding = response_dict['headers']['Content-Encoding'].lower()
            customized_response_dict["WasCompressed"] = encoding
            if encoding == 'gzip':
                response_dict['body'] = gzip.decompress(response_dict['body'])
            if encoding == 'deflate':
                response_dict['body'] = zlib.decompress(response_dict['body'])
            customized_response_dict["DecompressedLength"] = len(response_dict['body'])
        
    # Register the decompression handler - this runs before parsing
    with register_event_handler(dynamodb, 'before-parse', decompress_response):
        yield

# Context manager to enable/disable Alternator's response compression, by setting response size compression threshold.
# We can't do it for DynamoDB, in that case it is a no-op.
@contextmanager
def server_response_compression(dynamodb, compression_threshold=DDB_RESPONSE_COMPRESSION_THRESHOLD, compression_level=6):
    with scylla_config_temporary(dynamodb, f'alternator_response_compression_threshold_in_bytes', str(compression_threshold), nop=is_aws(dynamodb)):
        with scylla_config_temporary(dynamodb, f'alternator_response_gzip_compression_level', str(compression_level), nop=is_aws(dynamodb)):
            yield

# Basic test for compressed responses from the server.
# This test works for both Alternator and DynamoDB.
# In this test, we check that with 'Accept-Encoding' header in the request
# responses above DynamoDBs threshold are compressed with the requested encoding and smaller ones are not.
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_compressed_response(dynamodb, test_table_s, encoding):
    p = random_string()
    # single column slightly below threshold, but full row response will be above it
    compressible_data = 'x' * (DDB_RESPONSE_COMPRESSION_THRESHOLD // 2)
    test_table_s.put_item(Item={'p': p, 'a': compressible_data, 'b': compressible_data, 'c': compressible_data})
                
    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb):

            # First, verify that without the 'Accept-Encoding: gzip' header, we get normal response
            response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='a, b, c')
            assert all(response['Item'][col] == compressible_data for col in ('a', 'b', 'c'))
            assert "WasCompressed" not in response

            # Now check the compression case
            with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
                response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='a, b, c')
                assert all(response['Item'][col] == compressible_data for col in ('a', 'b', 'c'))
                assert "WasCompressed" in response
                assert response["WasCompressed"] == encoding
                # Not very precise, but check that compression actually happened
                assert response["DecompressedLength"] > 30 * response["HTTPContentLength"]

                # But also check that responses below threshold are not compressed
                # even if 'Accept-Encoding' header is present.
                response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='a')
                assert response['Item']['a'] == compressible_data
                assert "WasCompressed" not in response

# Similar to `test_compressed_response`, but now we also test with less compressible data, 
# which result in larger response that will not fit in single buffer and will be compressed in pieces. 
# With this test we should see if we can avoid large allocations.
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_compressed_response_large(dynamodb, test_table_s, encoding):
    p = random_string()
    large_slightly_compressible_data = random_string(length=DDB_RESPONSE_COMPRESSION_THRESHOLD * 50)
    test_table_s.put_item(Item={'p': p, 'y': large_slightly_compressible_data})
                
    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb):
            with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
                response = test_table_s.get_item(Key={'p': p}, ProjectionExpression='y')
                assert response['Item']['y'] == large_slightly_compressible_data
                assert "WasCompressed" in response
                assert response["WasCompressed"] == encoding
                # Not very precise, but check that some compression actually happened
                assert response["DecompressedLength"] > 1.2 * response["HTTPContentLength"]

# Separate test to check that compression works also for chunked responses.
# To trigger chunked responses in Alternator, we can use the BatchGetItem with response above 100KB.
# DynamoDB probably does not use chunked responses, so we don't require it in that case.
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_chunked_response_compression(dynamodb, test_table_s, encoding):
    # 10 items with 15KB data each - should trigger chunked response in Alternator
    data = {random_string(): random_string(length=1024 * 15) for _ in range(10)}

    with test_table_s.batch_writer() as batch:
        for key,value in data.items():
            batch.put_item(Item={'p': key, 'x': value})

    # The request we will test:
    def get_batch():
        response = dynamodb.batch_get_item(RequestItems={
                test_table_s.name: {
                    'Keys': [{'p': key} for key in data]
                }
            }
        )
        v = response['Responses'][test_table_s.name]
        assert len(v) == len(data)
        for item in v:
            assert item['x'] == data[item['p']]
        return response
    
    # Check that in this test we exercised the chunked-encoding compression code-path of Alternator. 
    def assert_chunked(response_dict, **kwargs):
        if not is_aws(dynamodb):
            assert 'Transfer-Encoding' in response_dict['headers'] \
                    and response_dict['headers']['Transfer-Encoding'] == 'chunked'

    # Make sure compression is enabled for Alternator
    with server_response_compression(dynamodb):
        with client_response_decompression(dynamodb):
            with register_event_handler(dynamodb, 'before-parse', assert_chunked):
                # First, verify that without the 'Accept-Encoding' header, we get normal response
                response = get_batch()
                assert "WasCompressed" not in response

                with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
                    response = get_batch()
                    assert "WasCompressed" in response
                    assert response["WasCompressed"] == encoding
                    # Not very precise, but check that some compression actually happened
                    assert response["DecompressedLength"] > 1.2 * response["HTTPContentLength"]

# The test checks various values of 'Accept-Encoding' header.
# It works for both Alternator and DynamoDB.
@pytest.mark.xfail(reason='issue #27246')
def test_accept_encoding_header(dynamodb, test_table_s):
    p = random_string()

    compressible_data = 'x' * DDB_RESPONSE_COMPRESSION_THRESHOLD
    test_table_s.put_item(Item={'p': p, 'x': compressible_data})

    # Helper to check expected response compression with given custom headers
    def check_response_with_custom_headers(custom_headers, should_be_compressed):
        with request_custom_headers(dynamodb, custom_headers):
            response = test_table_s.get_item(Key={'p': p})
            assert response['Item']['x'] == compressible_data
            assert_context = f"with {custom_headers}"
            if should_be_compressed is None:
                assert assert_context and "WasCompressed" not in response
            else:
                assert assert_context and "WasCompressed" in response
                assert assert_context and response["WasCompressed"] == should_be_compressed

    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb):
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
            check_response_with_custom_headers({"Accept-Encoding": "br, identity, deflate, gzip"}, should_be_compressed='deflate') # Compared to the previous: order matters
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

            # some invalid values, but apparently not always fully rejected by DynamoDB
            check_response_with_custom_headers({"Accept-Encoding": "\"gzip\""}, should_be_compressed=None)
            check_response_with_custom_headers({"Accept-Encoding": "gzip br"}, should_be_compressed=None)
            check_response_with_custom_headers({"Accept-Encoding": "gzip:q=1"}, should_be_compressed=None)
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5, gzip;q=31"}, should_be_compressed="gzip") # although incorrect, quality > 1 treated as 1
            check_response_with_custom_headers({"Accept-Encoding": "gzip;q=31,deflate;q=51"}, should_be_compressed="gzip") # although incorrect, quality > 1 treated as 1
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5, gzip;q=0.2"}, should_be_compressed="deflate") # valid case - a control for the next one
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5, gzip;q=1;q=0.2"}, should_be_compressed="gzip") # although incorrect entry, first quality value is used in DynamoDB
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5,gzip;q=0."}, should_be_compressed="deflate") # valid case - a control for the next one
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5,gzip;q=0.x"}, should_be_compressed="gzip")
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=0.5,gzip;q=0. x"}, should_be_compressed="gzip")
            check_response_with_custom_headers({"Accept-Encoding": "deflate;q=-0.5,gzip;q=0.2"}, should_be_compressed="gzip")
            check_response_with_custom_headers({"Accept-Encoding": "gzip;q= 0;q=1"}, should_be_compressed=None)
            check_response_with_custom_headers({"Accept-Encoding": "gzip;q =0;q=1"}, should_be_compressed="gzip")
            check_response_with_custom_headers({"Accept-Encoding": "gzip; q=0;q=1"}, should_be_compressed=None)
            check_response_with_custom_headers({"Accept-Encoding": "gzip;nq=0;q=1"}, should_be_compressed=None)
            check_response_with_custom_headers({"Accept-Encoding": "gzip;q=0, gzip;q=1, gzip;q=0"}, should_be_compressed="gzip")
            check_response_with_custom_headers({"Accept-Encoding": "br,identity;q=0.5,gzip;q=0.0001"}, should_be_compressed="gzip") # although very small quality, still non-zero

# Accept-Encoding is a list of values, so it should be valid to provide it multiple times - recipient should combine all values.
# Boto3 (or requests/urllib3) is not able to create such a request, so we need to monkey-patch the low-level request generation code to inject multiple headers.
# It can be tricky with signing, so we test various cases here.
# It turns out that in DynamoDB headers are correctly combined for signature verification,
# but then it only uses the first Accept-Encoding header, which may be considered a bug.
# In Alternator, having multiple Accept-Encoding works well.
@pytest.mark.xfail(reason='issue #27246')
def test_multiple_accept_encoding_headers(dynamodb, test_table_s):
    p = random_string()
    compressible_data = 'x' * DDB_RESPONSE_COMPRESSION_THRESHOLD
    test_table_s.put_item(Item={'p': p, 'x': compressible_data})

    def check_replaced_accept_encoding_headers(inject_headers: list[tuple[str, str]], expected_compression = None):
        # Monkey-patch urllib's _make_request() functions for both HTTP and HTTPS,
        # which handles the low level message generation and lets us inject headers.
        # We replace "Accept-Encoding" headers with our own list of headers.
        original_http__make_request = urllib3.connectionpool.HTTPConnectionPool._make_request
        original_https__make_request = urllib3.connectionpool.HTTPSConnectionPool._make_request
        def patch_make_request(original_func):
            def wrapped_make_request(self, conn, method, url, *args, **kwargs):
                orig_putheader = conn.putheader
                def custom_putheader(name, value, *args, **kwargs):
                    if name.lower() == "accept-encoding":
                        for header_name, header_value in inject_headers:
                            orig_putheader(header_name, header_value, *args, **kwargs)
                    else:
                        orig_putheader(name, value, *args, **kwargs)
                conn.putheader = custom_putheader
                try:
                    return original_func(self, conn, method, url, *args, **kwargs)
                finally:
                    conn.putheader = orig_putheader
            return wrapped_make_request
        try:
            urllib3.connectionpool.HTTPConnectionPool._make_request = patch_make_request(original_http__make_request)
            urllib3.connectionpool.HTTPSConnectionPool._make_request = patch_make_request(original_https__make_request)
            response = test_table_s.get_item(Key={'p': p})
        finally:
            urllib3.connectionpool.HTTPConnectionPool._make_request = original_http__make_request
            urllib3.connectionpool.HTTPSConnectionPool._make_request = original_https__make_request
            dynamodb.meta.client._endpoint.http_session._manager.clear() # Exception may leave bad state in connection pool manager

        if expected_compression is None:
            assert f"with {inject_headers}" and "WasCompressed" not in response
        else:
            assert f"with {inject_headers}" and "WasCompressed" in response
            assert f"with {inject_headers}" and response["WasCompressed"] == expected_compression
        
    with client_response_decompression(dynamodb):
        with server_response_compression(dynamodb):
            # If Accept-Encoding is not set, boto3 adds 'identity' by default, but it is not used for signature then.
            check_replaced_accept_encoding_headers([("Accept-Encoding", "gzip")], expected_compression="gzip")
            check_replaced_accept_encoding_headers([("Accept-Encoding", "gzip"), ("Accept-Encoding", "identity")], expected_compression="gzip")
            # Only the first header is considered by DynamoDB, while Seastar handles it correctly:
            compression = None if is_aws(dynamodb) else "gzip"
            check_replaced_accept_encoding_headers([("Accept-Encoding", "identity"), ("Accept-Encoding", "gzip")], expected_compression=compression)

            # Now lets make it a part of signature
            # Replacing signed header causes signature errors in both DynamoDB and Alternator.
            with request_custom_headers(dynamodb, {"Accept-Encoding": "identity"}):
                with pytest.raises(ClientError, match="signature"): # InvalidSignatureException
                    check_replaced_accept_encoding_headers([("Accept-Encoding", b'gzip')], expected_compression=None)

            # We can split the header into multiple ones and it works with signing
            with request_custom_headers(dynamodb, {"Accept-Encoding": "gzip,identity"}):
                check_replaced_accept_encoding_headers([("Accept-Encoding", "gzip"), ("Accept-Encoding", "identity")], expected_compression="gzip")

            # But again DynamoDB actually uses only first one:
            with request_custom_headers(dynamodb, {"Accept-Encoding": "identity,gzip"}):
                check_replaced_accept_encoding_headers([("Accept-Encoding", "identity"), ("Accept-Encoding", "gzip")], expected_compression=compression)


# The following method, from the botocore.auth.SigV4Auth, describes how header values are normalized
# for signature computation:
#
#     def _header_value(self, value):
#         # From the sigv4 docs:
#         # Lowercase(HeaderName) + ':' + Trimall(HeaderValue)
#         #
#         # The Trimall function removes excess white space before and after
#         # values, and converts sequential spaces to a single space.
#         return ' '.join(value.split())
#
# This test verifies that the signature computation correctly trims and normalizes whitespace
# in the 'Accept-Encoding' header value, as required by the AWS sigv4 specification.
# If not handled correctly a 'wrong signature' error is returned.
# Reproduces #27775.
@pytest.mark.xfail(reason="issue #27775")
def test_signature_trims_accept_encoding_spaces(dynamodb, test_table_s):
    p = random_string()

    with request_custom_headers(dynamodb, {"Accept-Encoding": "  gzip,  deflate,    br  "}):
        test_table_s.get_item(Key={'p': p})

# So far we used DynamoDB's default threshold of 4KB for response compression and default compression level.
# Now test that in Alternator we can set this threshold, enable/disable compressions and set level.
@pytest.mark.xfail(reason='issue #27246')
@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_set_compression_options(dynamodb, test_table_s, scylla_only, encoding):
    p = random_string()

    compressible_data = 'ABC' * 200
    test_table_s.put_item(Item={'p': p, 'x': compressible_data})

    # Helper to check expected response compression with given thresholds
    def check_response_with_threshold(gzip_level, threshold, should_be_compressed):
        with server_response_compression(dynamodb, compression_threshold=threshold, compression_level=gzip_level):
                response = test_table_s.get_item(Key={'p': p})
                assert response['Item']['x'] == compressible_data
                assert_context = f"with options {(gzip_level, threshold)}"
                if should_be_compressed is None:
                    assert assert_context and "WasCompressed" not in response
                else:
                    assert assert_context and "WasCompressed" in response
                    assert assert_context and response["WasCompressed"] == should_be_compressed
                return response["HTTPContentLength"]

    with client_response_decompression(dynamodb):
        with request_custom_headers(dynamodb, {"Accept-Encoding": encoding}):
            # With compression disabled no compression and we can get actual response size
            response_length = check_response_with_threshold(gzip_level=0, threshold=1, should_be_compressed=None)

            # Now enable, but below threshold - no compression
            check_response_with_threshold(gzip_level=1, threshold=response_length+1, should_be_compressed=None)
            lo = check_response_with_threshold(gzip_level=1, threshold=response_length, should_be_compressed=encoding)
            mid = check_response_with_threshold(gzip_level=6, threshold=response_length, should_be_compressed=encoding)
            high = check_response_with_threshold(gzip_level=9, threshold=response_length, should_be_compressed=encoding)
            assert lo > high  # higher compression level should give smaller response
            assert lo >= mid >= high  # auto level should be between low and high
