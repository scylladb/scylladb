#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
from cassandra.policies import RoundRobinPolicy    # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_NAME       # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_VERSION    # type: ignore # pylint: disable=no-name-in-module

from test.cqlpy.conftest import host, cql, this_dc  # add required fixtures
from test.pylib.suite.python import add_host_option, add_cql_connection_options


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
    add_host_option(parser)
    add_cql_connection_options(parser)


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
