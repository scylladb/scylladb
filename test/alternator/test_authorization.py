# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for authorization

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

from test.alternator.test_manual_requests import get_signed_request


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
    # calculate a real signature, in this test we really a correct signature,
    # so we use a function we already have in test_manual_requests.py.
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Boto3 separates the components of the Authorization header by spaces.
    # Let's remove all of them except the first one (which separates the
    # signature algorithm name from the rest) and check the result still works:
    a = req.headers['Authorization'].split()
    req.headers['Authorization'] = a[0] + ' ' + ''.join(a[1:])
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.ok
