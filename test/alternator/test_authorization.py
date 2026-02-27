# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for authorization

import boto3
import pytest
import requests
import re
import random
from botocore.exceptions import ClientError

from .util import get_signed_request


# Test that trying to perform an operation signed with a wrong key
# will not succeed
def test_wrong_key_access(request, dynamodb):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    url = dynamodb.meta.client._endpoint.host
    with pytest.raises(ClientError, match='UnrecognizedClientException'):
        if url.endswith('.amazonaws.com'):
            boto3.client('dynamodb',endpoint_url=url, aws_access_key_id='wrong_id', aws_secret_access_key='x').describe_endpoints()
        else:
            verify = not url.startswith('https')
            boto3.client('dynamodb',endpoint_url=url, region_name='us-east-1', aws_access_key_id='whatever', aws_secret_access_key='x', verify=verify).describe_endpoints()

# A similar test, but this time the user is expected to exist in the database (for local tests)
def test_wrong_password(request, dynamodb):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    url = dynamodb.meta.client._endpoint.host
    with pytest.raises(ClientError, match='UnrecognizedClientException'):
        if url.endswith('.amazonaws.com'):
            boto3.client('dynamodb',endpoint_url=url, aws_access_key_id='alternator', aws_secret_access_key='wrong_key').describe_endpoints()
        else:
            verify = not url.startswith('https')
            boto3.client('dynamodb',endpoint_url=url, region_name='us-east-1', aws_access_key_id='alternator', aws_secret_access_key='wrong_key', verify=verify).describe_endpoints()

# A test ensuring that expired signatures are not accepted
def test_expired_signature(dynamodb, test_table):
    url = dynamodb.meta.client._endpoint.host
    print(url)
    headers = {'Content-Type': 'application/x-amz-json-1.0',
               'X-Amz-Date': '20170101T010101Z',
               'X-Amz-Target': 'DynamoDB_20120810.DescribeEndpoints',
               'Authorization': 'AWS4-HMAC-SHA256 Credential=cassandra/2/3/4/aws4_request SignedHeaders=x-amz-date;host Signature=123'
    }
    response = requests.post(url, headers=headers, verify=False)
    assert not response.ok
    assert "InvalidSignatureException" in response.text and "Signature expired" in response.text

# A test verifying that missing Authorization header results in an
# MissingAuthenticationTokenException error.
def test_no_authorization_header(dynamodb, test_table):
    url = dynamodb.meta.client._endpoint.host
    print(url)
    headers = {'Content-Type': 'application/x-amz-json-1.0',
               'X-Amz-Date': '20170101T010101Z',
               'X-Amz-Target': 'DynamoDB_20120810.DescribeEndpoints',
    }
    response = requests.post(url, headers=headers, verify=False)
    assert not response.ok
    assert "MissingAuthenticationTokenException" in response.text

# A test ensuring that signatures that exceed current time too much are not accepted.
# Watch out - this test is valid only for around next 1000 years, it needs to be updated later.
def test_signature_too_futuristic(dynamodb, test_table):
    url = dynamodb.meta.client._endpoint.host
    print(url)
    headers = {'Content-Type': 'application/x-amz-json-1.0',
               'X-Amz-Date': '30200101T010101Z',
               'X-Amz-Target': 'DynamoDB_20120810.DescribeEndpoints',
               'Authorization': 'AWS4-HMAC-SHA256 Credential=cassandra/2/3/4/aws4_request SignedHeaders=x-amz-date;host Signature=123'
    }
    response = requests.post(url, headers=headers, verify=False)
    assert not response.ok
    assert "InvalidSignatureException" in response.text and "Signature not yet current" in response.text

# A test that commas can be uses instead of whitespace to separate components
# of the Authorization headers - reproducing issue #9568.
def test_authorization_no_whitespace(dynamodb, test_table):
    # Unlike the above tests which checked error cases so didn't need to
    # calculate a real signature, in this test we really need a correct
    # signature, so we use the function get_signed_request().
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Boto3 separates the components of the Authorization header by spaces.
    # Let's remove all of them except the first one (which separates the
    # signature algorithm name from the rest) and check the result still works:
    a = req.headers['Authorization'].split()
    req.headers['Authorization'] = a[0] + ' ' + ''.join(a[1:])
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.ok

# AWS's SigV4 signing protocol "canonizes" the relevant headers before
# calculating the signature, as explained in the AWS documentation. This
# includes lowercasing header names, sorting the headers, removing excess
# spaces, separating multiple values by commas. See details in
# https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html#create-canonical-request
# The rationale for most of these "canonization" rules is that although
# middleboxes like HTTP proxies may not modify the request's body, they MAY
# modify headers and may modify headers in exactly these "innocent" ways like
# re-ordering the headers, and we don't want the signature to fail just
# because the request underwent such an innocent transformation.
#
# The following tests check that Alternator does this canonization correctly
# before calculating the signature. Boto3 uses this canonization when signing
# outgoing requests, so if it is missing on the Alternator side the signatures
# will not match and the request will be rejected and the test will fail.

# Test that in header values, trailing whitespace added after calculating
# the signature don't break the request.
def test_canonization_trailing_whitespace(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # get_signed_request() always adds and signs a "Content-Type" header
    # so we can modify that header - while keeping the calculated signature.
    assert 'Content-Type' in req.headers and 'content-type' in req.headers['Authorization']
    # Add some trailing spaces to the header
    req.headers['Content-Type'] = req.headers['Content-Type'] + '    '
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.ok

# Test that in header values, leading whitespace added after calculating
# the signature don't break the request.
def test_canonization_leading_whitespace(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    assert 'Content-Type' in req.headers and 'content-type' in req.headers['Authorization']
    # Add some leading spaces to the header
    req.headers['Content-Type'] = '    ' + req.headers['Content-Type']
    # Surprisingly, the Python "requests" library has check_header_validity()
    # that refuses leading whitespace in the header. Let's monkey-patch it
    # to avoid this check, so we can send this unusual (but legal HTTP) header.
    import requests.models
    orig = requests.models.check_header_validity
    requests.models.check_header_validity = lambda header: None
    try:
        response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
        assert response.ok
    finally:
        requests.models.check_header_validity = orig

# Strings of multiple whitespace in the middle of a header value were
# not correctly canonized in the past, and this test reproduces this
# bug (issue #27775)
def test_canonization_middle_whitespace(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    # We need a header that is one of the signed headers that has a space
    # in it so we can replace it by multiple spaces to check canonization.
    # By default, there is no such header but we can add one, "Meerkat":
    req = get_signed_request(dynamodb, 'PutItem', payload, {'Meerkat': 'Hello world'})
    # Sanity check, that we successfully put the "meerkat" header as one of
    # signed headers, and has a space in it:
    assert 'Meerkat' in req.headers
    assert 'meerkat' in req.headers['Authorization']
    assert ' ' in req.headers['Meerkat']
    # Replace the single space in the 'meerkat' header by multiple spaces
    req.headers['Meerkat'] = req.headers['Meerkat'].replace(' ', '   ', 1)
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.ok

# Test that the case of the header name is canonized before signing
def test_canonization_header_name_capitalization(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Confirm we have a signed "content-type" header that we can play with:
    assert 'Content-Type' in req.headers and 'content-type' in req.headers['Authorization']
    # Play with the header name's case:
    content_type = req.headers['Content-Type']
    del req.headers['Content-Type']
    req.headers['CoNtEnT-tYpE'] = content_type
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.ok

# Test that changing the header order doesn't break the signature
def test_canonization_header_order(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Find the list of signed headers inside the Authorization header
    m = re.search(r'SignedHeaders=([^, ]+)', req.headers['Authorization'])
    signed_header_names = m.group(1).split(';')
    saved_headers = {k: v for k,v in req.headers.items() if k.lower() in signed_header_names}
    # Create a new list of signed header names with the same capitalization
    # as in req.headers (in some versions of Python's "requests", req.headers
    # is not case-insensitive).
    signed_header_names = list(saved_headers.keys())
    for h in signed_header_names:
        del req.headers[h]
    # Put the headers back in a different order
    random.shuffle(signed_header_names)
    for h in signed_header_names:
        req.headers[h] = saved_headers[h]
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.ok
