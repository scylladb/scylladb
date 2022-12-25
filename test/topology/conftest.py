#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import asyncio
import logging
import ssl
from typing import List
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
from test.pylib.manager_client import ManagerClient, IPAddress
import pytest
from cassandra.cluster import Session, ResponseFuture                    # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Cluster, ConsistencyLevel                  # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore # pylint: disable=no-name-in-module
from cassandra.policies import RoundRobinPolicy                          # type: ignore
from cassandra.connection import DRIVER_NAME       # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_VERSION    # type: ignore # pylint: disable=no-name-in-module


logger = logging.getLogger(__name__)
logger.warning("Driver name %s", DRIVER_NAME)
logger.warning("Driver version %s", DRIVER_VERSION)


def pytest_addoption(parser):
    parser.addoption('--manager-api', action='store', required=True,
                     help='Manager unix socket path')
    parser.addoption('--host', action='store', default='localhost',
                     help='CQL server host to connect to')
    parser.addoption('--port', action='store', default='9042',
                     help='CQL server port to connect to')
    parser.addoption('--ssl', action='store_true',
                     help='Connect to CQL via an encrypted TLSv1.2 connection')

# Change default pytest-asyncio event_loop fixture scope to session to
# allow async fixtures with scope larger than function. (e.g. manager fixture)
# See https://github.com/pytest-dev/pytest-asyncio/issues/68
@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


def _wrap_future(f: ResponseFuture) -> asyncio.Future:
    """Wrap a cassandra Future into an asyncio.Future object.

    Args:
        f: future to wrap

    Returns:
        And asyncio.Future object which can be awaited.
    """
    loop = asyncio.get_event_loop()
    aio_future = loop.create_future()

    def on_result(result):
        if not aio_future.done():
            loop.call_soon_threadsafe(aio_future.set_result, result)
        else:
            logger.debug("_wrap_future: on_result() on already done future: %s", result)

    def on_error(exception, *_):
        if not aio_future.done():
            loop.call_soon_threadsafe(aio_future.set_exception, exception)
        else:
            logger.debug("_wrap_future: on_error() on already done future: %s"), result

    f.add_callback(on_result)
    f.add_errback(on_error)
    return aio_future


def run_async(self, *args, **kwargs) -> asyncio.Future:
    # The default timeouts should have been more than enough, but in some
    # extreme cases with a very slow debug build running on a slow or very busy
    # machine, they may not be. Observed tests reach 160 seconds. So it's
    # incremented to 200 seconds.
    # See issue #11289.
    kwargs.setdefault("timeout", 200.0)
    return _wrap_future(self.execute_async(*args, **kwargs))


Session.run_async = run_async


# cluster_con helper: set up client object for communicating with the CQL API.
def cluster_con(hosts: List[IPAddress], port: int, use_ssl: bool):
    """Create a CQL Cluster connection object according to configuration.
       It does not .connect() yet."""
    assert len(hosts) > 0, "python driver connection needs at least one host to connect to"
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeouts should have been more than enough, but in some
        # extreme cases with a very slow debug build running on a slow or very busy
        # machine, they may not be. Observed tests reach 160 seconds. So it's
        # incremented to 200 seconds.
        # See issue #11289.
        # NOTE: request_timeout is the main cause of timeouts, even if logs say heartbeat
        request_timeout=200)
    if use_ssl:
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None

    return Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                   contact_points=hosts,
                   port=port,
                   # TODO: make the protocol version an option, to allow testing with
                   # different versions. If we drop this setting completely, it will
                   # mean pick the latest version supported by the client and the server.
                   protocol_version=4,
                   # NOTE: No auth provider as auth keysppace has RF=1 and topology will take
                   # down nodes, causing errors. If auth is needed in the future for topology
                   # tests, they should bump up auth RF and run repair.
                   ssl_context=ssl_context,
                   # The default timeouts should have been more than enough, but in some
                   # extreme cases with a very slow debug build running on a slow or very busy
                   # machine, they may not be. Observed tests reach 160 seconds. So it's
                   # incremented to 200 seconds.
                   # See issue #11289.
                   connect_timeout = 200,
                   control_connection_timeout = 200,
                   # NOTE: max_schema_agreement_wait must be 2x or 3x smaller than request_timeout
                   # else the driver can't handle a server being down
                   max_schema_agreement_wait=20,
                   idle_heartbeat_timeout=200,
                   )


@pytest.mark.asyncio
@pytest.fixture(scope="session")
async def manager_internal(event_loop, request):
    """Session fixture to set up client object for communicating with the Cluster API.
       Pass the Unix socket path where the Manager server API is listening.
       Pass a function to create driver connections.
       Test cases (functions) should not use this fixture.
    """
    port = int(request.config.getoption('port'))
    use_ssl = bool(request.config.getoption('ssl'))
    manager_int = ManagerClient(request.config.getoption('manager_api'), port, use_ssl, cluster_con)
    yield manager_int
    await manager_int.stop()  # Stop client session and close driver after last test

@pytest.fixture(scope="function")
async def manager(request, manager_internal):
    """Per test fixture to notify Manager client object when tests begin so it can
    perform checks for cluster state.
    """
    test_case_name = request.node.name
    await manager_internal.before_test(test_case_name)
    yield manager_internal
    await manager_internal.after_test(test_case_name)

# "cql" fixture: set up client object for communicating with the CQL API.
# Since connection is managed by manager just return that object
@pytest.fixture(scope="function")
def cql(manager):
    yield manager.cql

# Consistent schema change feature is optionally enabled and
# some tests are expected to fail on Scylla without this
# option enabled, and pass with it enabled (and also pass on Cassandra).
# These tests should use the "fails_without_consistent_cluster_management"
# fixture. When consistent mode becomes the default, this fixture can be removed.
@pytest.fixture(scope="function")
def check_pre_consistent_cluster_management(manager):
    # If not running on Scylla, return false.
    names = [row.table_name for row in manager.cql.execute(
            "SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
    if not any('scylla' in name for name in names):
        return False
    # In Scylla, we check Raft mode by inspecting the configuration via CQL.
    consistent = list(manager.cql.execute("SELECT value FROM system.config WHERE name = 'consistent_cluster_management'"))
    return len(consistent) == 0 or consistent[0].value == "false"


@pytest.fixture(scope="function")
def fails_without_consistent_cluster_management(request, check_pre_consistent_cluster_management):
    if check_pre_consistent_cluster_management:
        request.node.add_marker(pytest.mark.xfail(reason="Test expected to fail without consistent cluster management "
                                                         "feature on"))


# "random_tables" fixture: Creates and returns a temporary RandomTables object
# used in tests to make schema changes. Tables are dropped after finished.
@pytest.fixture(scope="function")
def random_tables(request, manager):
    tables = RandomTables(request.node.name, manager, unique_name())
    yield tables
    tables.drop_all()
