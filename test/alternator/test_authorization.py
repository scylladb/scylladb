# Copyright 2019 ScyllaDB
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

# Tests for authorization

import pytest
import botocore
from botocore.exceptions import ClientError
import boto3
import requests

# Test that trying to perform an operation signed with a wrong key
# will not succeed
def test_wrong_key_access(request, dynamodb):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    url = dynamodb.meta.client._endpoint.host
    with pytest.raises(ClientError, match='UnrecognizedClientException'):
        if url.endswith('.amazonaws.com'):
            boto3.client('dynamodb',endpoint_url=url, aws_access_key_id='wrong_id', aws_secret_access_key='').describe_endpoints()
        else:
            verify = not url.startswith('https')
            boto3.client('dynamodb',endpoint_url=url, region_name='us-east-1', aws_access_key_id='whatever', aws_secret_access_key='', verify=verify).describe_endpoints()

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
               'Authorization': 'AWS4-HMAC-SHA256 Credential=alternator/2/3/4/aws4_request SignedHeaders=x-amz-date;host Signature=123'
    }
    response = requests.post(url, headers=headers, verify=False)
    assert not response.ok
    assert "InvalidSignatureException" in response.text and "Signature expired" in response.text

# A test verifying that missing Authorization header is handled properly
def test_no_authorization_header(dynamodb, test_table):
    url = dynamodb.meta.client._endpoint.host
    print(url)
    headers = {'Content-Type': 'application/x-amz-json-1.0',
               'X-Amz-Date': '20170101T010101Z',
               'X-Amz-Target': 'DynamoDB_20120810.DescribeEndpoints',
    }
    response = requests.post(url, headers=headers, verify=False)
    assert not response.ok
    assert "InvalidSignatureException" in response.text and "Authorization header" in response.text

# A test ensuring that signatures that exceed current time too much are not accepted.
# Watch out - this test is valid only for around next 1000 years, it needs to be updated later.
def test_signature_too_futuristic(dynamodb, test_table):
    url = dynamodb.meta.client._endpoint.host
    print(url)
    headers = {'Content-Type': 'application/x-amz-json-1.0',
               'X-Amz-Date': '30200101T010101Z',
               'X-Amz-Target': 'DynamoDB_20120810.DescribeEndpoints',
               'Authorization': 'AWS4-HMAC-SHA256 Credential=alternator/2/3/4/aws4_request SignedHeaders=x-amz-date;host Signature=123'
    }
    response = requests.post(url, headers=headers, verify=False)
    assert not response.ok
    assert "InvalidSignatureException" in response.text and "Signature not yet current" in response.text
