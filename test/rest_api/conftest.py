# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an individual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest
import requests

from test.cqlpy.conftest import host, cql, this_dc  # add required fixtures
from test.cqlpy.util import unique_name, new_test_keyspace, keyspace_has_tablets, is_scylla
from test.pylib.runner import testpy_test_fixture_scope
from test.pylib.suite.python import add_host_option, add_cql_connection_options

# By default, tests run against a Scylla server listening
# on localhost:9042 for CQL and localhost:10000 for the REST API.
# Add the --host, --port, --ssl, or --api-port options to allow overriding these defaults.
def pytest_addoption(parser):
    add_host_option(parser)
    add_cql_connection_options(parser)
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
@pytest.fixture(scope=testpy_test_fixture_scope)
def rest_api(request, host):
    port = request.config.getoption('api_port')
    return RestApiSession(host, port)

# The "scylla_only" fixture can be used by tests for Scylla-only features,
# which do not exist on Apache Cassandra. A test using this fixture will be
# skipped if running with "run-cassandra".
@pytest.fixture(scope=testpy_test_fixture_scope)
def scylla_only(cql):
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    if not is_scylla(cql):
        pytest.skip('Scylla-only test skipped')

@pytest.fixture(scope=testpy_test_fixture_scope)
def has_tablets(cql):
    with new_test_keyspace(cql, " WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace:
        return keyspace_has_tablets(cql, keyspace)

@pytest.fixture(scope="function")
def skip_with_tablets(has_tablets):
    if has_tablets:
        pytest.skip("Test may crash with tablets experimental feature on")

@pytest.fixture(scope="function")
def skip_without_tablets(scylla_only, has_tablets):
    if not has_tablets:
        pytest.skip("Test needs tablets experimental feature on")

@pytest.fixture(scope=testpy_test_fixture_scope)
def test_keyspace_vnodes(cql, this_dc, has_tablets):
    name = unique_name()
    if has_tablets:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}")
    else:
        # If tablets are not available or not enabled, we just create a regular keyspace
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    cql.execute("DROP KEYSPACE " + name)

@pytest.fixture(scope=testpy_test_fixture_scope)
def test_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    cql.execute("DROP KEYSPACE " + name)
