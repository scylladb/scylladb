# Copyright 2021-present ScyllaDB
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

# Tests for CORS (Cross-origin resource sharing) protocol support.
# If the request has the "Origin" header specifying where the script which
# makes this request comes from, Alternator needs to reply with the header
# "Access-Control-Allow-Origin: *" saying that this (and any) origin is fine.
# Moreover, in a "preflight" request - with the OPTIONS method - the script
# can also "request" in headers that the server allows it to use some
# headers and HTTP methods in the followup request, and the server should
# respond by "allowing" them in the response headers.

import requests

# If the request does not have a "Origin" header, the reply should not
# have any of the CORS headers. We test this for the GET, POST and
# OPTIONS methods.
def test_cors_not_used(dynamodb):
    cors_headers = [
        'Access-Control-Allow-Origin',
        'Access-Control-Allow-Credentials',
        'Access-Control-Expose-Headers',
        'Access-Control-Max-Age',
        'Access-Control-Allow-Methods',
        'Access-Control-Allow-Headers']
    url = dynamodb.meta.client._endpoint.host
    for f in [requests.options, requests.get, requests.post]:
        response = f(url, verify=False)
        for h in cors_headers:
            assert not h in response.headers

# If the request has the "Origin" header, the reply must have the
# 'Access-Control-Allow-Origin: *' header, saying that this origin
# (and any other) is fine.
def test_cors_allow_origin(dynamodb):
    headers = {'Origin': 'http://example.com/'}
    url = dynamodb.meta.client._endpoint.host
    for f in [requests.options, requests.get, requests.post]:
        response = f(url, headers=headers, verify=False)
        assert 'Access-Control-Allow-Origin' in response.headers
        assert response.headers['Access-Control-Allow-Origin'] == '*'
        assert 'Access-Control-Expose-Headers' in response.headers
        # Only the pre-flight request has the Access-Control-Max-Age
        # header
        if f == requests.options:
            assert 'Access-Control-Max-Age' in response.headers
        else:
            assert not 'Access-Control-Max-Age' in response.headers

# If this is a CORS request (has Origin header) pre-flight (i.e., OPTIONS
# method), the reply should "echo" the requested headers and requested
# methods as allowed headers and allowed methods. Other method types
# ignore these headers, and don't need to echo them.
def test_cors_allow_requested(dynamodb):
    headers = {
        'Origin': 'http://example.com/',
        'Access-Control-Request-Method': 'dog',
        'Access-Control-Request-Headers': 'cat, meerkat, mouse'
    }
    url = dynamodb.meta.client._endpoint.host
    for f in [requests.options, requests.get, requests.post]:
        response = f(url, headers=headers, verify=False)
        assert 'Access-Control-Allow-Origin' in response.headers
        assert response.headers['Access-Control-Allow-Origin'] == '*'
        assert 'Access-Control-Expose-Headers' in response.headers
        # Only the pre-flight request has the Access-Control-Max-Age
        # header and echo the "requested" headers and methods to the
        # reply as "allowed" headers with the same content.
        if f == requests.options:
            assert 'Access-Control-Max-Age' in response.headers
            assert 'Access-Control-Allow-Methods' in response.headers
            assert response.headers['Access-Control-Allow-Methods'] == headers['Access-Control-Request-Method']
            assert 'Access-Control-Allow-Headers' in response.headers
            assert response.headers['Access-Control-Allow-Headers'] == headers['Access-Control-Request-Headers']
        else:
            assert not 'Access-Control-Max-Age' in response.headers
            assert not 'Access-Control-Allow-Methods' in response.headers
            assert not 'Access-Control-Allow-Headers' in response.headers
