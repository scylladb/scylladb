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

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an invididual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest
import requests

# By default, tests run against a Scylla server listening
# on localhost:9042 for CQL and localhost:10000 for the REST API.
# Add the --host, --port, --ssl, or --api-port options to allow overiding these defaults.
def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
        help='Scylla server host to connect to')
    parser.addoption('--port', action='store', default='9042',
        help='Scylla CQL port to connect to')
    parser.addoption('--ssl', action='store_true',
        help='Connect to CQL via an encrypted TLSv1.2 connection')
    parser.addoption('--api-port', action='store', default='10000',
        help='server REST API port to connect to')

class RestApiSession:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.session = requests.Session()

    def send(self, method, path, params={}):
        url=f"http://{self.host}:{self.port}/{path}"
        if params:
            sep = '?'
            for key, value in params.items():
                url += f"{sep}{key}={value}"
                sep = '&'
        req = self.session.prepare_request(requests.Request(method, url))
        return self.session.send(req)

# "api" fixture: set up client object for communicating with Scylla API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 10000, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def rest_api(request):
    host = request.config.getoption('host')
    port = request.config.getoption('api_port')
    return RestApiSession(host, port)
