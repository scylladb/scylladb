# Copyright 2019-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the health check

import requests

from .util import get_cert

# Test that a health check can be performed with a GET packet
def test_health_works(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    response = requests.get(url, verify=False, cert=get_cert(dynamodb))
    assert response.ok
    assert response.content.decode('utf-8').strip()  == 'healthy: {}'.format(url.replace('https://', '').replace('http://', ''))

# Test that a health check only works for the root URL ('/')
def test_health_only_works_for_root_path(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    for suffix in ['/abc', '/-', '/index.htm']:
        print(url + suffix)
        response = requests.get(url + suffix, verify=False, cert=get_cert(dynamodb))
        assert response.status_code in range(400, 405)
