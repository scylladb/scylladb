# Copyright 2019-present ScyllaDB
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

# Tests for the health check

import requests

# Test that a health check can be performed with a GET packet
def test_health_works(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    response = requests.get(url, verify=False)
    assert response.ok
    assert response.content.decode('utf-8').strip()  == 'healthy: {}'.format(url.replace('https://', '').replace('http://', ''))

# Test that a health check only works for the root URL ('/')
def test_health_only_works_for_root_path(dynamodb):
    url = dynamodb.meta.client._endpoint.host
    for suffix in ['/abc', '/-', '/index.htm', '/health']:
        print(url + suffix)
        response = requests.get(url + suffix, verify=False)
        assert response.status_code in range(400, 405)
