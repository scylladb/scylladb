#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING
from test import TOP_SRC_DIR, MODES_TIMEOUT_FACTOR, path_to
from test.pylib.runner import PHASE_REPORT_KEY, MANAGER_LOGS_KEY, make_failed_test_dir
from test.pylib.object_storage import make_object_storage, format_tuples
from test.pylib.random_tables import RandomTables
from test.pylib.skip_types import skip_env
from test.cluster.util import FeatureConfig
from test.pylib.util import unique_name
from test.pylib.async_cql import run_async
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_scylla_2025_1_description
from test.pylib.connect_options import add_cql_connection_options
from test.pylib.encryption_provider import KeyProvider, make_key_provider_factory
import logging
import pytest
from cassandra.auth import PlainTextAuthProvider                         # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Session                                    # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_NAME       # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_VERSION    # type: ignore # pylint: disable=no-name-in-module
from collections.abc import AsyncIterator

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


    from test.pylib.scylla_cluster import ClusterFactory


Session.run_async = run_async     # patch Session for convenience


logger = logging.getLogger(__name__)

print(f"Driver name {DRIVER_NAME}, version {DRIVER_VERSION}")


async def decode_backtrace(build_mode: str, input: str):
    executable = Path(path_to(build_mode, "scylla"))
    proc = await asyncio.create_subprocess_exec(
        (TOP_SRC_DIR / "seastar" / "scripts" / "seastar-addr2line").absolute(),
        "-e",
        executable.absolute(),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(input=input.encode())
    return f"{stdout.decode()}\n{stderr.decode()}"


def pytest_addoption(parser):
    add_cql_connection_options(parser)
    parser.addoption('--skip-internet-dependent-tests', action='store_true', default=False,
                     help='Skip tests which depend on artifacts from the internet')


@pytest.fixture(scope="module")
async def _scylla_cluster_manager(request: pytest.FixtureRequest,
                                  suite_log_dir: Path,
                                  testpy_cluster_factory: ClusterFactory,
                                  testpy_uname: str) -> AsyncGenerator[ScyllaClusterManager]:
    """Run the cluster manager on its own thread and event loop.

    The manager owns loop-bound state -- the Scylla subprocess transports and
    the per-server asyncio locks -- so all of its coroutines have to run on one
    loop, and that loop has to keep running: tests reach the manager from
    pytest's event loops and, in dtest, from plain worker threads.  Hence, a
    dedicated thread whose loop is idle but alive until teardown.
    """
    auth_username = request.config.getoption('auth_username', default=None)
    auth_password = request.config.getoption('auth_password', default=None)
    if auth_username is not None and auth_password is not None:
        auth_provider = PlainTextAuthProvider(username=auth_username, password=auth_password)
    else:
        auth_provider = None

    ready: concurrent.futures.Future[ScyllaClusterManager] = concurrent.futures.Future()
    stop_event = threading.Event()

    async def run_manager() -> None:
        mgr = ScyllaClusterManager(
            test_uname=testpy_uname,
            create_cluster=testpy_cluster_factory,
            base_dir=str(suite_log_dir),
            port=int(request.config.getoption('port')),
            use_ssl=bool(request.config.getoption('ssl')),
            auth_provider=auth_provider,
        )
        try:
            await mgr.start()
        except BaseException:
            # Dispose of a partially started manager, e.g. a cluster
            # created before the API site failed to start.
            await mgr.stop()
            raise
        ready.set_result(mgr)
        try:
            await asyncio.get_running_loop().run_in_executor(None, stop_event.wait)
        finally:
            await mgr.stop()

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="cluster-manager") as executor:
        future = executor.submit(asyncio.run, run_manager())
        # Fail instead of waiting forever when the manager dies before it
        # signals readiness, e.g. because creating the first cluster failed.
        # The callback fires only once the future is done, so reaching it
        # without a result always means a startup failure.
        future.add_done_callback(
            lambda f: None if ready.done() else ready.set_exception(
                f.exception() or RuntimeError("ScyllaClusterManager exited before signaling readiness")))
        # ready.result() blocks, so hand it to a thread rather than stalling
        # the loop this fixture runs on.
        server = await asyncio.get_running_loop().run_in_executor(None, ready.result)
        try:
            yield server
        finally:
            stop_event.set()
            # Wait for the manager thread off the event loop.  Stopping the
            # manager recycles the cluster, and recycling runs teardown
            # callbacks that belong to this loop; blocking it here would
            # deadlock them.
            await asyncio.get_running_loop().run_in_executor(None, future.result)


@pytest.fixture(scope="function")
async def manager(request: pytest.FixtureRequest,
                  _scylla_cluster_manager: ScyllaClusterManager,
                  build_mode: str) -> AsyncGenerator[ScyllaClusterManager]:
    """
    Per test fixture to notify the manager when tests begin so it can perform checks for cluster state.
    """
    test_case_name = request.node.name

    logger.debug("before_test for %s", test_case_name)
    cluster_str = await _scylla_cluster_manager.before_test(test_case_name)
    logger.info(f"Using cluster: {cluster_str} for test {test_case_name}")
    if _scylla_cluster_manager.cql is None and await _scylla_cluster_manager.running_servers():
        await _scylla_cluster_manager.driver_connect()  # Connect driver to the leased cluster

    # Publish what pytest_runtest_makereport needs to attach this test's logs on
    # failure (single source of truth), so it doesn't re-derive these paths.
    # The pytest session log is not listed here: it is written per xdist worker
    # (see PYTEST_LOG_FILE in test/pylib/runner.py) and is already linked from the
    # failed test's properties by record_failed_test_artifacts().
    request.node.stash[MANAGER_LOGS_KEY] = {
        "manager": _scylla_cluster_manager,
        "logs": {"test_py.log": _scylla_cluster_manager.test_case_log_file},
    }
    yield _scylla_cluster_manager
    # `request.node.stash` contains reports stored per phase in `pytest_runtest_makereport`
    # from where we can retrieve test failure.
    cluster_status = None
    found_errors = {}
    failed = False
    failed_test_dir_path = None
    try:
        reports = request.node.stash[PHASE_REPORT_KEY]
        call_report = reports.get("call")
        failed = call_report is not None and call_report.failed

        # Check if the test has the check_nodes_for_errors marker
        found_errors = await _scylla_cluster_manager.check_all_errors(all_errors=(request.node.get_closest_marker("check_nodes_for_errors") is not None))

        if failed or found_errors:
            # Server logs / traceback / links are attached by pytest_runtest_makereport;
            # here we only need the dir for the manager-specific found_errors files below.
            failed_test_dir_path = make_failed_test_dir(request.config, build_mode, test_case_name)

    finally:
        # Drop the stash entry before closing the driver so a teardown-phase
        # failure report doesn't gather logs through a fenced-off manager.
        request.node.stash[MANAGER_LOGS_KEY] = None

    # Close the driver before after_test() raises the fence: the session is
    # this test's, and nothing in after_test() needs it -- the keyspace count
    # post-condition uses each server's own control connection.  after_test()
    # runs even if closing fails: it is what raises the fence, detaches this
    # test's log handler and hands the cluster back.
    try:
        _scylla_cluster_manager.driver_close()
    finally:
        # Tear down (after test): notify the manager that the test finished.
        # This also cuts off manager access for tasks leaked by the test.
        logger.debug("after_test for %s (success: %s)", test_case_name, not failed)
        cluster_status = await _scylla_cluster_manager.after_test(success=not failed)
        logger.info("Cluster after test %s (success: %s): %s", test_case_name, not failed, cluster_status)

    if cluster_status is not None and cluster_status["server_broken"] and not failed:
        failed = True
        pytest.fail(
            f"test case {test_case_name} left unfinished tasks on Scylla server. Server marked as broken,"
            f" server_broken_reason: {cluster_status["message"]}"
        )
    if found_errors:
        full_message = []
        for server, data in found_errors.items():
            summary = []
            detailed = []

            if criticals := data.get("critical", []):
                summary.append(f"{len(criticals)} critical error(s)")
                detailed.extend(map(str.rstrip, criticals))

            if backtraces := data.get("backtraces", []):
                summary.append(f"{len(backtraces)} backtrace(s)")
                with open(failed_test_dir_path / f"scylla-{server.server_id}-backtraces.txt", "w") as bt_file:
                    for backtrace in backtraces:
                        bt_file.write(backtrace + "\n\n")
                        decoded_bt = await decode_backtrace(build_mode, backtrace)
                        bt_file.write(decoded_bt + "\n\n")
                    detailed.append(f"{len(backtraces)} backtrace(s) saved in {Path(bt_file.name).name}")

            if errors := data.get("error", []):
                summary.append(f"{len(errors)} error(s)")
                detailed.extend(map(str.rstrip, errors))

            if cores := data.get("cores", []):
                summary.append(f"{len(cores)} core(s): {', '.join(cores)}")

            if summary:
                summary_line = f"Server {server.server_id}: found {', '.join(summary)} (log: { data['log']})"
                detailed = [f"  {line}" for line in detailed]
                full_message.append(summary_line)
                full_message.extend(detailed)

        with open(failed_test_dir_path / "found_errors.txt", "w") as f:
            f.write("\n".join(full_message))
        if not failed:
            pytest.fail(f"\n{'\n'.join(full_message)}")

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
    reports = request.node.stash[PHASE_REPORT_KEY]
    call_report = reports.get("call")
    failed = call_report is not None and call_report.failed
    if not failed and not await manager.is_dirty():
        tables.drop_all()

@pytest.fixture(scope="function", autouse=True)
async def prepare_3_nodes_cluster(request, manager):
    if request.node.get_closest_marker("prepare_3_nodes_cluster"):
        await manager.servers_add(3)


@pytest.fixture(scope="function", autouse=True)
async def prepare_3_racks_cluster(request, manager):
    if request.node.get_closest_marker("prepare_3_racks_cluster"):
        await manager.servers_add(3, auto_rack_dc="dc1")


@pytest.fixture(scope="function")
def internet_dependency_enabled(request) -> None:
    if request.config.getoption('skip_internet_dependent_tests'):
        skip_env(reason="skip_internet_dependent_tests is set")


@pytest.fixture(scope="function")
async def scylla_2025_1(request, build_mode, internet_dependency_enabled) -> AsyncIterator[ScyllaVersionDescription]:
    yield await get_scylla_2025_1_description(build_mode)

@pytest.fixture(scope="function", params=list(KeyProvider))
async def key_provider(request, tmpdir, suite_log_dir, scylla_binary):
    """Encryption providers fixture"""
    async with make_key_provider_factory(request.param, tmpdir, suite_log_dir, scylla_binary) as res:
        yield res


@pytest.fixture(scope="function")
def failure_detector_timeout(build_mode):
    return 5000 * MODES_TIMEOUT_FACTOR[build_mode]

@pytest.fixture(params=[None, 's3', 'gs'], ids=['local', 's3', 'gs'])
async def storage_config(request, pytestconfig, tmpdir, suite_log_dir, manager: ScyllaClusterManager) -> FeatureConfig:
    """Parametrize tests over local / S3 / GCS storage, as a FeatureConfig.

    A test applies the object-storage cluster config and keyspace STORAGE clause
    the same way it applies any other configuration, and combines the two by
    chaining: storage_config.get_cluster_cfg(feature_config.get_cluster_cfg(cfg)).
    The local flavor yields an empty configuration.
    """
    if request.param is None:
        yield FeatureConfig()
        return

    # Skip before provisioning: a test that skips itself in its body has already
    # paid for a backend, and leaves the cluster clean, so the teardown callback
    # that stops the backend never fires.
    for marker in request.node.iter_markers('skip_storage'):
        reason = marker.kwargs.get('reason')
        if not reason:
            raise pytest.UsageError(f"{request.node.name}: skip_storage takes the flavors positionally "
                                    "and requires reason=...")
        if request.param in marker.args:
            skip_env(reason)

    async with make_object_storage(request.param, pytestconfig, tmpdir, suite_log_dir, request.node.name, manager) as server:
        storage_opts = format_tuples(type=server.type,
                                     endpoint=server.address,
                                     bucket=server.bucket_name)
        yield FeatureConfig(ks_opts=f" WITH STORAGE = {storage_opts}",
                            cluster_cfg={'object_storage_endpoints': server.create_endpoint_conf()},
                            on_object_storage=True)
