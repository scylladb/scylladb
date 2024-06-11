# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use. A "fixture" is some
# setup which an individual test requires to run; The fixture has setup code
# and teardown code, and if multiple tests require the same fixture, it can
# be set up only once - while still allowing the user to run individual tests
# and automatically setting up the fixtures they need.

import pytest
import requests
import ssl
import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ConsistencyLevel, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy

from test.pylib.report_plugin import ReportPlugin

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/test/cql-pytest')
from util import unique_name, new_test_keyspace, keyspace_has_tablets, is_scylla

# By default, tests run against a Scylla server listening
# on localhost:9042 for CQL and localhost:10000 for the REST API.
# Add the --host, --port, --ssl, or --api-port options to allow overriding these defaults.
def pytest_addoption(parser):
    parser.addoption('--host', action='store', default='localhost',
        help='Scylla server host to connect to')
    parser.addoption('--port', action='store', default='9042',
        help='Scylla CQL port to connect to')
    parser.addoption('--ssl', action='store_true',
        help='Connect to CQL via an encrypted TLSv1.2 connection')
    parser.addoption('--api-port', action='store', default='10000',
        help='server REST API port to connect to')
    parser.addoption('--mode', action='store', required=True,
                     help='Scylla build mode. Tests can use it to adjust their behavior.')
    parser.addoption('--run_id', action='store', default=1,
                     help='Run id for the test run')

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

# "cql" fixture: set up client object for communicating with the CQL API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 9042, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def cql(request):
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeout (in seconds) for execute() commands is 10, which
        # should have been more than enough, but in some extreme cases with a
        # very slow debug build running on a very busy machine and a very slow
        # request (e.g., a DROP KEYSPACE needing to drop multiple tables)
        # 10 seconds may not be enough, so let's increase it. See issue #7838.
        request_timeout = 120)
    if request.config.getoption('ssl'):
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[request.config.getoption('host')],
        port=int(request.config.getoption('port')),
        # TODO: make the protocol version an option, to allow testing with
        # different versions. If we drop this setting completely, it will
        # mean pick the latest version supported by the client and the server.
        protocol_version=4,
        # Use the default superuser credentials, which work for both Scylla and Cassandra
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
        ssl_context=ssl_context,
        # The default timeout for new connections is 5 seconds, and for
        # requests made by the control connection is 2 seconds. These should
        # have been more than enough, but in some extreme cases with a very
        # slow debug build running on a very busy machine, they may not be.
        # so let's increase them to 60 seconds. See issue #11289.
        connect_timeout = 60,
        control_connection_timeout = 60,
    )
    return cluster.connect()

# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will have
# a "this_dc" fixture to figure out the name of the current DC, so it can be
# used in NetworkTopologyStrategy.
@pytest.fixture(scope="session")
def this_dc(cql):
    yield cql.execute("SELECT data_center FROM system.local").one()[0]

# The "scylla_only" fixture can be used by tests for Scylla-only features,
# which do not exist on Apache Cassandra. A test using this fixture will be
# skipped if running with "run-cassandra".
@pytest.fixture(scope="session")
def scylla_only(cql):
    # We recognize Scylla by checking if there is any system table whose name
    # contains the word "scylla":
    if not is_scylla(cql):
        pytest.skip('Scylla-only test skipped')

@pytest.fixture(scope="session")
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

@pytest.fixture(scope="session")
def test_keyspace_vnodes(cql, this_dc, has_tablets):
    name = unique_name()
    if has_tablets:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 } AND TABLETS = {'enabled': false}")
    else:
        # If tablets are not available or not enabled, we just create a regular keyspace
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    cql.execute("DROP KEYSPACE " + name)

@pytest.fixture(scope="session")
def test_keyspace(cql, this_dc):
    name = unique_name()
    cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "' : 1 }")
    yield name
    cql.execute("DROP KEYSPACE " + name)


def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())
