# Copyright 2020 ScyllaDB
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

# Tests for manual requests - not necessarily generated
# by boto3, in order to allow non-validated input to get through

import pytest
import requests
from botocore.exceptions import BotoCoreError, ClientError

def gen_json(n):
    return '{"":'*n + '{}' + '}'*n

def get_signed_request(dynamodb, target, payload):
    # NOTE: Signing routines use boto3 implementation details and may be prone
    # to unexpected changes
    class Request:
        url=dynamodb.meta.client._endpoint.host
        headers={'X-Amz-Target': 'DynamoDB_20120810.' + target}
        body=payload.encode(encoding='UTF-8')
        method='POST'
        context={}
        params={}
    req = Request()
    signer = dynamodb.meta.client._request_signer
    signer.get_auth(signer.signing_name, signer.region_name).add_auth(request=req)
    return req

# Test that deeply nested objects (e.g. with depth of 200k) are parsed correctly,
# i.e. do not cause stack overflows for the server. It's totally fine for the
# server to refuse these packets with an error message though.
# NOTE: The test uses raw HTTP requests, because it's not easy to send
# a deeply nested object via boto3 - it quickly crashes on 'too deep recursion'
# for objects with depth as low as 150 (with sys.getrecursionlimit() == 3000).
# Hence, a request is manually crafted to contain a deeply nested JSON document.
def test_deeply_nested_put(dynamodb, test_table):
    big_json = gen_json(200000)
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}, "attribute":' + big_json + '}}'

    req = get_signed_request(dynamodb, 'PutItem', payload)
    # Check that the request delivery succeeded and the server
    # responded with a comprehensible message - it can be either
    # a success report or an error - both are acceptable as long as
    # the oversized message did not make the server crash.
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    print(response, response.text)

    # If the PutItem request above failed, the deeply nested item
    # was not put into the database, so it's fine for this request
    # to receive a response that it was not found. An error informing
    # about not being able to process this request is also acceptable,
    # as long as the server didn't crash.
    item = test_table.get_item(Key={'p': 'x', 'c':'x'})
    print(item)

# Test that a too deeply nested object is refused,
# assuming max depth of 32 - and keeping the nested level
# low enough for Python not to choke on it with too deep recursion
def test_exceed_nested_level_a_little(dynamodb, test_table):
    p = 'xxx'
    c = 'yyy'
    nested = dict()
    nested_it = nested
    for i in range(50):
        nested_it['a'] = dict()
        nested_it = nested_it['a']
    with pytest.raises(ClientError, match='.*Exception.*nested'):
        test_table.put_item(Item={'p': p, 'c': c, 'nested': nested})


def test_too_large_request(dynamodb, test_table):
    p = 'abc'
    c = 'def'
    big = 'x' * (16 * 1024 * 1024 + 7)
    # The exception type differs due to differences between HTTP servers
    # in alternator and DynamoDB. The former returns 413, the latter
    # a ClientError explaining that the element size was too large.
    with pytest.raises(BotoCoreError):
        test_table.put_item(Item={'p': p, 'c': c, 'big': big})

def test_incorrect_json(dynamodb, test_table):
    correct_req = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'

    # Check all non-full prefixes of a correct JSON - none of them are valid JSON's themselves
    # NOTE: DynamoDB returns two kinds of errors on incorrect input - SerializationException
    # or "Page Not Found". Alternator returns "ValidationExeption" for simplicity.
    validate_resp = lambda t: "SerializationException" in t or "ValidationException" in t or "Page Not Found" in t
    for i in range(len(correct_req)):
        req = get_signed_request(dynamodb, 'PutItem', correct_req[:i])
        response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
        assert validate_resp(response.text)

    incorrect_reqs = [
        '}}}', '}{', 'habababa', '7', '124463gwe', '><#', '????', '"""', '{"""}', '{""}', '{7}',
        '{3: }}', '{"2":{}', ',', '{,}', '{{}}', '"a": "b"', '{{{', '{'*10000 + '}'*9999, '{'*10000 + '}'*10007
    ]
    for incorrect_req in incorrect_reqs:
        req = get_signed_request(dynamodb, 'PutItem', incorrect_req)
        response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
        assert validate_resp(response.text)
