# Copyright 2020-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Tests for manual requests - not necessarily generated
# by boto3, in order to allow non-validated input to get through

import base64
import json

import pytest
import requests
import urllib3
from botocore.exceptions import BotoCoreError, ClientError
from packaging.version import Version

from test.alternator.util import random_bytes


def gen_json(n):
    return '{"":'*n + '{}' + '}'*n

def get_signed_request(dynamodb, target, payload):
    # NOTE: Signing routines use boto3 implementation details and may be prone
    # to unexpected changes
    class Request:
        url=dynamodb.meta.client._endpoint.host
        headers={'X-Amz-Target': 'DynamoDB_20120810.' + target, 'Content-Type': 'application/x-amz-json-1.0'}
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
    item = test_table.get_item(Key={'p': 'x', 'c':'x'}, ConsistentRead=True)
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

# Test that we indeed allow the maximum level of 32 nested objects
def test_almost_exceed_nested_level(dynamodb, test_table):
    p = 'xxx'
    c = 'yyy'
    nested = dict()
    nested_it = nested
    for i in range(30): # 30 added levels + top level + the item itself == 32 total
        nested_it['a'] = dict()
        nested_it = nested_it['a']
    test_table.put_item(Item={'p': p, 'c': c, 'nested': nested})

def test_too_large_request(dynamodb, test_table):
    p = 'abc'
    c = 'def'
    big = 'x' * (16 * 1024 * 1024 + 7)
    # The exception type differs due to differences between HTTP servers
    # in alternator and DynamoDB. The former returns 413, the latter
    # a ClientError explaining that the element size was too large.
    with pytest.raises(BotoCoreError):
        try:
            test_table.put_item(Item={'p': p, 'c': c, 'big': big})
        except ClientError:
            raise BotoCoreError()

# Tests that a request larger than the 16MB limit is rejected, improving on
# the rather blunt test in test_too_large_request() above. The following two
# tests verify that:
# 1. An over-long request is rejected no matter if it is sent using a
#    Content-Length header or chunked encoding (reproduces issue #8196).
# 2. The client should be able to recognize this error as a 413 error, not
#     some I/O error like broken pipe (reproduces issue #8195).
@pytest.mark.xfail(reason="issue #8196, #12166")
def test_too_large_request_chunked(dynamodb, test_table):
    if Version(urllib3.__version__) < Version('1.26'):
        pytest.skip("urllib3 before 1.26.0 threw broken pipe and did not read response and cause issue #8195. Fixed by pull request urllib3/urllib3#1524")
    # To make a request very large, we just stuff it with a lot of spaces :-)
    spaces = ' ' * (17 * 1024 * 1024)
    req = get_signed_request(dynamodb, 'PutItem',
        '{"TableName": "' + test_table.name + '", ' + spaces + '"Item": {"p": {"S": "x"}, "c": {"S": "x"}}}')
    def generator(s):
        yield s
    response = requests.post(req.url, headers=req.headers, data=generator(req.body), verify=False)
    # In issue #8196, Alternator did not recognize the request is too long
    # because it uses chunked encoding instead of Content-Length, so the
    # request succeeded, and the status_code was 200 instead of 413.
    assert response.status_code == 413

@pytest.mark.xfail(reason="issue #12166. Note that only fails very rairly, will usually xpass")
def test_too_large_request_content_length(dynamodb, test_table):
    if Version(urllib3.__version__) < Version('1.26'):
        pytest.skip("urllib3 before 1.26.0 threw broken pipe and did not read response and cause issue #8195. Fixed by pull request urllib3/urllib3#1524")
    spaces = ' ' * (17 * 1024 * 1024)
    req = get_signed_request(dynamodb, 'PutItem',
        '{"TableName": "' + test_table.name + '", ' + spaces + '"Item": {"p": {"S": "x"}, "c": {"S": "x"}}}')
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    # In issue #8195, Alternator closed the connection early, causing the
    # library to incorrectly throw an exception (Broken Pipe) instead noticing
    # the error code 413 which the server did send.
    assert response.status_code == 413

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

# Test that the value returned by PutItem is always a JSON object, not an empty string (see #6568)
def test_put_item_return_type(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    req = get_signed_request(dynamodb, 'PutItem', payload)
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.text
    # json::loads throws on invalid input
    json.loads(response.text)

# Test that TagResource and UntagResource requests return empty HTTP body on success
def test_tags_return_empty_body(dynamodb, test_table):
    descr = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    arn =  descr['TableArn']
    req = get_signed_request(dynamodb, 'TagResource', '{"ResourceArn": "' + arn + '", "Tags": [{"Key": "k", "Value": "v"}]}')
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert not response.text
    req = get_signed_request(dynamodb, 'UntagResource', '{"ResourceArn": "' + arn + '", "TagKeys": ["k"]}')
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert not response.text

# Test that incorrect number values are detected
def test_incorrect_numbers(dynamodb, test_table):
    for incorrect in ["NaN", "Infinity", "-Infinity", "-NaN", "dog", "-dog"]:
        payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}, "v": {"N": "' + incorrect + '"}}}'
        req = get_signed_request(dynamodb, 'PutItem', payload)
        response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
        assert "ValidationException" in response.text and "numeric" in response.text

# Although the DynamoDB API responses are JSON, additional conventions apply
# to these responses - such as how error codes are encoded in JSON. For this
# reason, DynamoDB uses the content type 'application/x-amz-json-1.0' instead
# of the standard 'application/json'. This test verifies that we return the
# correct content type header.
# While most DynamoDB libraries we tried do not care about an unexpected
# content-type, it turns out that one (aiodynamo) does. Moreover, AWS already
# defined x-amz-json-1.1 - see
#    https://awslabs.github.io/smithy/1.0/spec/aws/aws-json-1_1-protocol.html
# which differs (only) in how it encodes error replies.
# So in the future it may become even more important that Scylla return the
# correct content type.
def test_content_type(dynamodb, test_table):
    payload = '{"TableName": "' + test_table.name + '", "Item": {"p": {"S": "x"}, "c": {"S": "x"}}}'
    # Note that get_signed_request() uses x-amz-json-1.0 to encode the
    # *request*. In the future this may or may not effect the content type
    # in the response (today, DynamoDB doesn't allow any other content type
    # in the request anyway).
    req = get_signed_request(dynamodb, 'PutItem', payload)
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.headers['Content-Type'] == 'application/x-amz-json-1.0'

# An unknown operation should result with an UnknownOperationException:
def test_unknown_operation(dynamodb):
    req = get_signed_request(dynamodb, 'BoguousOperationName', '{}')
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.status_code == 400
    assert 'UnknownOperationException' in response.text
    print(response.text)

# Reproduce issue #10278, where double-quotes in an error message resulted
# in a broken JSON structure in the error message, which confuses boto3 to
# misunderstand the error response.
# Because this test uses boto3, we can reproduce the error, but not really
# understand what it is. We have another variant of this test below -
# test_exception_escape_raw() - that does the same thing without boto3
# so we can see the error happens during the response JSON parsing.
def test_exception_escape(test_table_s):
    # ADD expects its parameter :inc to be an integer. We'll send a string,
    # so expect a ValidationException.
    with pytest.raises(ClientError) as error:
        test_table_s.update_item(Key={'p': 'hello'},
            UpdateExpression='ADD n :inc',
            ExpressionAttributeValues={':inc': '1'})
    r = error.value.response
    assert r['Error']['Code'] == 'ValidationException'
    assert r['ResponseMetadata']['HTTPStatusCode'] == 400

# Similar to test_exception_escape above, but do the request manually,
# without boto3. This avoids boto3's blundering attempts of covering up
# the error it seens - and allows us to notice that the bug is that
# Alternator returns an unparsable JSON response.
# Reproduces #10278
def test_exception_escape_raw(dynamodb, test_table_s):
    payload = '{"TableName": "' + test_table_s.name + '", "Key": {"p": {"S": "hello"}}, "UpdateExpression": "ADD n :inc", "ExpressionAttributeValues": {":inc": {"S": "1"}}}'
    req = get_signed_request(dynamodb, 'UpdateItem', payload)
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    assert response.status_code == 400
    # In issue #10278, the JSON parsing fails:
    r = json.loads(response.text)
    assert 'ValidationException' in r['__type']

def put_item_binary_data_in_key(dynamodb, test_table_b, item_data):
    payload = '{"TableName": "%s", "Item": {"p": {"B": "%s"}}}' % (test_table_b.name, item_data)
    req = get_signed_request(dynamodb, 'PutItem', payload)
    return requests.post(req.url, headers=req.headers, data=req.body, verify=True)

def put_item_binary_data_in_non_key(dynamodb, test_table_b, item_data):
    payload ='''{
        "TableName": "%s",
        "Item": {
            "p": {
                "B": "%s"
            },
            "c": {
                "B": "%s"
            }
        }
    }''' % (test_table_b.name, base64.b64encode(random_bytes()).decode(), item_data)
    req = get_signed_request(dynamodb, 'PutItem', payload)
    return requests.post(req.url, headers=req.headers, data=req.body, verify=True)

# Reproduces issue #6487 where setting binary values with missing "=" padding characters
# was allowed in Scylla.
def test_base64_missing_padding(dynamodb, test_table_b):
    r = put_item_binary_data_in_key(dynamodb, test_table_b, "YWJjZGVmZ2g")
    assert r.status_code == 400
    r = put_item_binary_data_in_non_key(dynamodb, test_table_b, "YWJjZGVmZ2g")
    assert r.status_code == 400

# Tests the case where non base64 text is placed as binary data value.
def test_base64_malformed(dynamodb, test_table_b):
    r = put_item_binary_data_in_key(dynamodb, test_table_b, "YWJj??!!")
    assert r.status_code == 400
    r = put_item_binary_data_in_non_key(dynamodb, test_table_b, "YWJj??!!")
    assert r.status_code == 400

def scan_with_binary_data_in_cond_expr(dynamodb, test_table_b, filter_expr, expr_attr_values):
    payload ='''{
        "TableName": "%s",
        "FilterExpression": "%s",
        "ExpressionAttributeValues": { %s }
    }''' % (test_table_b.name, filter_expr, expr_attr_values)
    req = get_signed_request(dynamodb, 'Scan', payload)
    return requests.post(req.url, headers=req.headers, data=req.body, verify=True)

# Tests the case where malformed binary data is placed as part of filter expression
def test_base64_malformed_cond_expr(dynamodb, test_table_b):
    # put some data
    c_data = base64.b64encode(b"fefe").decode()
    r = put_item_binary_data_in_non_key(dynamodb, test_table_b, c_data)
    assert r.status_code == 200

    malformed_data = "ZmVmZQ=!" # has the same length as c_data to test begins_with
    exp_attr = '''":v": {"B": "%s"}''' % malformed_data

    # note that expression "c = :v" or "c in(:v)" would fail on dynamodb but not on alternator
    # as we don't deserialize in this case
    for exp in [
            "c > :v",
            ":v > c",
            "NOT c > :v",
            "contains(c, :v)",
            "contains(:v, c)",
            "c between :v and :v",
            ":v between c and c",
            "begins_with(c, :v)",
            "begins_with(:v, c)"]:
        r = scan_with_binary_data_in_cond_expr(dynamodb, test_table_b, exp, exp_attr)
        assert r.status_code == 400, "Failed on expression \"%s\"" % (exp)
