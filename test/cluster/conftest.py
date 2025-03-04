#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use
import pathlib
import ssl
import platform
import urllib.parse
from functools import partial
from typing import List, Optional, Dict
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
from test.pylib.manager_client import ManagerClient, IPAddress
from test.pylib.async_cql import event_loop, run_async
import logging
import pytest
from cassandra.auth import PlainTextAuthProvider                         # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Session                                    # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Cluster, ConsistencyLevel                  # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore # pylint: disable=no-name-in-module
from cassandra.policies import ExponentialReconnectionPolicy             # type: ignore
from cassandra.policies import RoundRobinPolicy                          # type: ignore
from cassandra.policies import TokenAwarePolicy                          # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy                 # type: ignore
from cassandra.connection import DRIVER_NAME       # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_VERSION    # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import EndPoint          # type: ignore # pylint: disable=no-name-in-module


Session.run_async = run_async     # patch Session for convenience


logger = logging.getLogger(__name__)

print(f"Driver name {DRIVER_NAME}, version {DRIVER_VERSION}")


def pytest_addoption(parser):
    parser.addoption('--manager-api', action='store', required=True,
                     help='Manager unix socket path')
    parser.addoption('--host', action='store', default='localhost',
                     help='CQL server host to connect to')
    parser.addoption('--port', action='store', default='9042',
                     help='CQL server port to connect to')
    parser.addoption('--ssl', action='store_true',
                     help='Connect to CQL via an encrypted TLSv1.2 connection')
    parser.addoption('--auth_username', action='store', default=None,
                        help='username for authentication')
    parser.addoption('--auth_password', action='store', default=None,
                        help='password for authentication')
    parser.addoption('--artifacts_dir_url', action='store', type=str, default=None, dest='artifacts_dir_url',
                     help='Provide the URL to artifacts directory to generate the link to failed tests directory '
                          'with logs')


# This is a constant used in `pytest_runtest_makereport` below to store the full report for the test case
# in a stash which can then be accessed from fixtures to print the stacktrace for the failed test
PHASE_REPORT_KEY = pytest.StashKey[Dict[str, pytest.CollectReport]]()


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """This is a post-test hook executed by the pytest library.
    Use it to access the test result and store a flag indicating failure
    so we can later retrieve it in our fixtures like `manager`.

    `item.stash` is the same stash as `request.node.stash` (in the `request`
    fixture provided by pytest).
    """
    outcome = yield
    report = outcome.get_result()

    item.stash[PHASE_REPORT_KEY] = report


conn_logger = logging.getLogger("conn_messages")
conn_logger.setLevel(logging.INFO)

class CustomConnection(Cluster.connection_class):
    def send_msg(self, *args, **argv):
        conn_logger.debug(f"send_msg: ({id(self)}): {args} {argv}")
        return super(CustomConnection, self).send_msg(*args, **argv)

    def process_msg(self, msg, protocol_version):
        conn_logger.debug(f"process_msg: ({id(self)}): {msg}")
        return super(CustomConnection, self).process_msg(msg, protocol_version)


# cluster_con helper: set up client object for communicating with the CQL API.
def cluster_con(hosts: List[IPAddress | EndPoint], port: int, use_ssl: bool, auth_provider=None, load_balancing_policy=RoundRobinPolicy()):
    """Create a CQL Cluster connection object according to configuration.
       It does not .connect() yet."""
    assert len(hosts) > 0, "python driver connection needs at least one host to connect to"
    profile = ExecutionProfile(
        load_balancing_policy=load_balancing_policy,
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeouts should have been more than enough, but in some
        # extreme cases with a very slow debug build running on a slow or very busy
        # machine, they may not be. Observed tests reach 160 seconds. So it's
        # incremented to 200 seconds.
        # See issue #11289.
        # NOTE: request_timeout is the main cause of timeouts, even if logs say heartbeat
        request_timeout=200)
    whitelist_profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(WhiteListRoundRobinPolicy(hosts)),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200)
    if use_ssl:
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None

    return Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile, 'whitelist': whitelist_profile},
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
                   # The default reconnection policy has a large maximum interval
                   # between retries (600 seconds). In tests that restart/replace nodes,
                   # where a node can be unavailable for an extended period of time,
                   # this can cause the reconnection retry interval to get very large,
                   # longer than a test timeout.
                   reconnection_policy = ExponentialReconnectionPolicy(1.0, 4.0),

                   auth_provider=auth_provider,
                   # Capture messages for debugging purposes.
                   connection_class=CustomConnection
                   )


@pytest.fixture(scope="session")
async def manager_internal(event_loop, request):
    """Session fixture to prepare client object for communicating with the Cluster API.
       Pass the Unix socket path where the Manager server API is listening.
       Pass a function to create driver connections.
       Test cases (functions) should not use this fixture.
    """
    port = int(request.config.getoption('port'))
    use_ssl = bool(request.config.getoption('ssl'))
    auth_username = request.config.getoption('auth_username', default=None)
    auth_password = request.config.getoption('auth_password', default=None)
    if auth_username is not None and auth_password is not None:
        auth_provider = PlainTextAuthProvider(username=auth_username, password=auth_password)
    else:
        auth_provider = None
    manager_int = partial(ManagerClient, request.config.getoption('manager_api'), port, use_ssl, auth_provider, cluster_con)
    yield manager_int


@pytest.fixture(scope="function")
async def manager(request, manager_internal, record_property, build_mode, is_test_needed_cluster):
    """Per test fixture to notify Manager client object when tests begin so it can
    perform checks for cluster state.
    """
    test_case_name = request.node.name
    run_id = request.config.getoption('run_id')
    tmp_dir = pathlib.Path(request.config.getoption('tmpdir'))
    xml_path: pathlib.Path = pathlib.Path(request.config.getoption('xmlpath'))
    suite_testpy_log = (tmp_dir /
                        build_mode /
                        f"{pathlib.Path(xml_path.stem).stem}.log"
                        )
    test_log = suite_testpy_log.parent / f"{suite_testpy_log.stem}.{test_case_name}.log"
    # this should be consistent with scylla_cluster.py handler name in _before_test method
    test_py_log_test = suite_testpy_log.parent / f"{suite_testpy_log.stem}_{test_case_name}_cluster.log"

    manager_client = manager_internal()  # set up client object in fixture with scope function
    if not is_test_needed_cluster:
        await manager_client.mark_dirty()
    await manager_client.before_test(test_case_name, test_log)
    yield manager_client
    # `request.node.stash` contains a report stored in `pytest_runtest_makereport` from where we can retrieve
    # test failure.
    report = request.node.stash[PHASE_REPORT_KEY]
    failed = report.when == "call" and report.failed
    if failed:
        # Save scylladb logs for failed tests in a separate directory and copy XML report to the same directory to have
        # all related logs in one dir.
        # Then add property to the XML report with the path to the directory, so it can be visible in Jenkins
        failed_test_dir_path = tmp_dir / build_mode / "failed_test" / f"{test_case_name}"
        failed_test_dir_path.mkdir(parents=True, exist_ok=True)
        await manager_client.gather_related_logs(
            failed_test_dir_path,
            {'pytest.log': test_log, 'test_py.log': test_py_log_test}
        )
        with open(failed_test_dir_path / f"stacktrace", 'w') as f:
            f.write(report.longreprtext)
        if request.config.getoption('artifacts_dir_url') is not None:
            # get the relative path to the tmpdir for the failed directory
            dir_path_relative = f"{failed_test_dir_path.as_posix()[failed_test_dir_path.as_posix().find('testlog'):]}"
            full_url = urllib.parse.urljoin(request.config.getoption('artifacts_dir_url') + '/',
                                            urllib.parse.quote(dir_path_relative))
            record_property("TEST_LOGS", f"<a href={full_url}>failed_test_logs</a>")

    cluster_status = await manager_client.after_test(test_case_name, not failed)
    await manager_client.stop()  # Stop client session and close driver after each test
    if cluster_status["server_broken"]:
        pytest.fail(f"test case {test_case_name} leave unfinished tasks on Scylla server. Server marked as broken, server_broken_reason: {cluster_status["message"]}")

# "cql" fixture: set up client object for communicating with the CQL API.
# Since connection is managed by manager just return that object
@pytest.fixture(scope="function")
def cql(manager):
    yield manager.cql

# "random_tables" fixture: Creates and returns a temporary RandomTables object
# used in tests to make schema changes. Tables are dropped after test finishes
# unless the cluster is dirty or the test has failed.
@pytest.fixture(scope="function")
async def random_tables(request, manager):
    rf_marker = request.node.get_closest_marker("replication_factor")
    replication_factor = rf_marker.args[0] if rf_marker is not None else 3  # Default 3
    enable_tablets = request.node.get_closest_marker("enable_tablets")
    enable_tablets = enable_tablets.args[0] if enable_tablets is not None else None
    tables = RandomTables(request.node.name, manager, unique_name(),
                          replication_factor, None, enable_tablets)
    yield tables

    # Don't drop tables at the end if we failed or the cluster is dirty - it may be impossible
    # (e.g. the cluster is completely dead) and it doesn't matter (we won't reuse the cluster
    # anyway).
    # The cluster will be marked as dirty if the test failed, but that happens
    # at the end of `manager` fixture which we depend on (so these steps will be
    # executed after us) - so at this point, we need to check for failure ourselves too.
    report = request.node.stash[PHASE_REPORT_KEY]
    failed = report.when == "call" and report.failed
    if not failed and not await manager.is_dirty():
        tables.drop_all()

skipped_funcs = {}
# Can be used to mark a test to be skipped for a specific mode=[release, dev, debug]
# The reason to skip a test should be specified, used as a comment only.
# Additionally, platform_key can be specified to limit the scope of the attribute
# to the specified platform. Example platform_key-s: [aarch64, x86_64]
def skip_mode(mode: str, reason: str, platform_key: Optional[str]=None):
    def wrap(func):
        skipped_funcs.setdefault((func, mode), []).append((reason, platform_key))
        return func
    return wrap

@pytest.fixture(scope="function", autouse=True)
def skip_mode_fixture(request, build_mode):
    for reason, platform_key in skipped_funcs.get((request.function, build_mode), []):
        if platform_key is None or platform_key in platform.platform():
            pytest.skip(f'{request.node.name} skipped, reason: {reason}')


@pytest.fixture(scope="function")
def is_test_needed_cluster(request):
    prepare_marker = request.node.get_closest_marker("prepare_3_nodes_cluster")
    return bool(prepare_marker)


@pytest.fixture(scope="function", autouse=True)
async def prepare_3_nodes_cluster(is_test_needed_cluster, manager):
    servers = await  manager.running_servers()
    if not servers and is_test_needed_cluster:
        await manager.servers_add(3)
        await manager.mark_clean()