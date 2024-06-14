#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

"""
Pytest fixtures for CQL repl tests.
"""

import logging
import ssl
import uuid
import pytest
from cassandra.auth import PlainTextAuthProvider                         # type: ignore
from cassandra.cluster import Cluster              # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import ConsistencyLevel     # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile     # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import EXEC_PROFILE_DEFAULT  # pylint: disable=no-name-in-module
from cassandra.policies import RoundRobinPolicy    # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_NAME       # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_VERSION    # type: ignore # pylint: disable=no-name-in-module

from test.pylib.report_plugin import ReportPlugin

logger = logging.getLogger(__name__)
logger.warning("Driver name %s", DRIVER_NAME)
logger.warning("Driver version %s", DRIVER_VERSION)


# By default, tests run against a CQL server (Scylla or Cassandra) listening
# on localhost:9042. Add the --host and --port options to allow overriding
# these defaults.
def pytest_addoption(parser) -> None:
    """Set up command line parameters"""
    parser.addoption("--input", action="store", default="",
                     help="Input file")
    parser.addoption("--output", action="store", default="",
                     help="Output file")
    parser.addoption('--host', action='store', default='localhost',
        help='CQL server host to connect to')
    parser.addoption('--port', action='store', default='9042',
        help='CQL server port to connect to')
    parser.addoption('--ssl', action='store_true',
        help='Connect to CQL via an encrypted TLSv1.2 connection')
    parser.addoption('--mode', action='store', required=True,
                     help='Scylla build mode. Tests can use it to adjust their behavior.')
    parser.addoption('--run_id', action='store', default=1,
                     help='Run id for the test run')


# "cql" fixture: set up client object for communicating with the CQL API.
# The host/port combination of the server are determined by the --host and
# --port options, and defaults to localhost and 9042, respectively.
# We use scope="session" so that all tests will reuse the same client object.
@pytest.fixture(scope="session")
def cql(request):
    """A shared CQL connection object for all tests in this pytest session"""
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeout (in seconds) for execute() commands is 10, which
        # should have been more than enough, but in some extreme cases with a
        # very slow debug build running on a very busy machine and a very slow
        # request (e.g., a DROP KEYSPACE needing to drop multiple tables)
        # 10 seconds may not be enough, so let's increase it. See issue #7838.
        request_timeout=120)
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
                      # Use default superuser credentials, which work for both Scylla and Cassandra
                      auth_provider=PlainTextAuthProvider(username='cassandra',
                                                          password='cassandra'),
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


# A function-scoped autouse=True fixture allows us to test after every test
# that the CQL connection is still alive - and if not report the test which
# crashed Scylla and stop running any more tests.
@pytest.fixture(scope="function", autouse=True)
def cql_test_connection(cql, request):  # pylint: disable=redefined-outer-name
    """Fixture to test Cassandra/Scylla is still up after tests finished"""
    yield
    try:
        # We want to run a do-nothing CQL command. "use system" is the
        # closest to do-nothing I could find...
        cql.execute("use system")
    except Exception:     # noqa: E722     pylint: disable=broad-except
        pytest.exit("Scylla appears to have crashed in test "
                    f"{request.node.parent.name}::{request.node.name}")


# Until Cassandra 4, NetworkTopologyStrategy did not support the option
# replication_factor (https://issues.apache.org/jira/browse/CASSANDRA-14303).
# We want to allow these tests to run on Cassandra 3.* (for the convenience
# of developers who happen to have it installed), so we'll use the older
# syntax that needs to specify a DC name explicitly. For this, will have
# a "this_dc" fixture to figure out the name of the current DC, so it can be
# used in NetworkTopologyStrategy.
@pytest.fixture(scope="session")
def this_dc(cql):                       # pylint: disable=redefined-outer-name
    """Fixture to get current DC, for backwards compatibility"""
    yield cql.execute("SELECT data_center FROM system.local").one()[0]


# "" fixture: Creates and returns a temporary keyspace to be
# used in tests that need a keyspace. The keyspace is created with RF=1,
# and automatically deleted at the end. We use scope="session" so that all
# tests will reuse the same keyspace.
@pytest.fixture(scope="session")
def keyspace(cql, this_dc):             # pylint: disable=redefined-outer-name
    """Fixture to create a test kespace for this pytest session"""
    keyspace_name = f"test_{uuid.uuid4().hex}"
    cql.execute(f"CREATE KEYSPACE {keyspace_name} "
                f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    yield keyspace_name
    cql.execute(f"DROP KEYSPACE {keyspace_name}")


def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())
