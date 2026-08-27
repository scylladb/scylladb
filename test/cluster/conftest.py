#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING
from test import TOP_SRC_DIR, MODES_TIMEOUT_FACTOR, path_to
from test.pylib.runner import PHASE_REPORT_KEY, MANAGER_LOGS_KEY, make_failed_test_dir
from test.cluster.object_store.conftest import make_object_storage
from test.pylib.random_tables import RandomTables
from test.pylib.skip_types import skip_env
from test.pylib.util import unique_name
from test.pylib.async_cql import run_async
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_scylla_2025_1_description
from test.pylib.connect_options import add_cql_connection_options, add_s3_options
from test.pylib.encryption_provider import KeyProvider, make_key_provider_factory
import logging
import pytest
from cassandra.cluster import Session                                    # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_NAME       # type: ignore # pylint: disable=no-name-in-module
from cassandra.connection import DRIVER_VERSION    # type: ignore # pylint: disable=no-name-in-module
from collections.abc import AsyncIterator

SCRIPTS_DIR = str(TOP_SRC_DIR / "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

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
    add_s3_options(parser)
    parser.addoption('--skip-internet-dependent-tests', action='store_true', default=False,
                     help='Skip tests which depend on artifacts from the internet')


@pytest.fixture
async def manager(request: pytest.FixtureRequest,
                  testpy_cluster_factory: ClusterFactory,
                  testpy_logger: logging.Logger,
                  testpy_test_name: str) -> AsyncGenerator[ScyllaClusterManager]:
    """
    Per test cluster manager: connects the driver, and on teardown checks the
    server logs and reports the test's verdict to the manager.
    """
    test_case_name = request.node.name

    cluster = await testpy_cluster_factory(testpy_logger)

    async with ScyllaClusterManager(
            test_name=testpy_test_name,
            cluster=cluster,
            port=int(request.config.getoption("--port")),
            use_ssl=bool(request.config.getoption("--ssl")),
            auth_username=request.config.getoption("--auth_username", default=None),
            auth_password=request.config.getoption("--auth_password", default=None),
    ).run_in_thread() as mgr:
        if request.node.get_closest_marker("prepare_3_nodes_cluster"):
            await mgr.servers_add(3)
        if request.node.get_closest_marker("prepare_3_racks_cluster"):
            await mgr.servers_add(3, auto_rack_dc="dc1")

        if mgr.cql is None and await mgr.running_servers():
            await mgr.driver_connect()  # Connect driver to the test's cluster

        # Publish what pytest_runtest_makereport needs to attach this test's logs on
        # failure (single source of truth), so it doesn't re-derive these paths.
        request.node.stash[MANAGER_LOGS_KEY] = {
            "manager": mgr,
            "logs": {},
        }
        yield mgr
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
            found_errors = await mgr.check_all_errors(all_errors=(request.node.get_closest_marker("check_nodes_for_errors") is not None))

            if failed or found_errors:
                # Server logs / traceback / links are attached by pytest_runtest_makereport;
                # here we only need the dir for the manager-specific found_errors files below.
                failed_test_dir_path = make_failed_test_dir(request.config, mgr.cluster.mode, test_case_name)

        finally:
            # Drop the stash entry before closing the driver so a teardown-phase
            # failure report doesn't gather logs through a manager being stopped.
            request.node.stash[MANAGER_LOGS_KEY] = None

        # Close the driver before after_test(): the session is this test's, and
        # nothing in after_test() needs it -- the keyspace count post-condition
        # uses each server's own control connection.  after_test() runs even if
        # closing fails: it detaches this test's log handler and hands the
        # cluster back.
        try:
            mgr.driver_close()
        finally:
            # Tear down (after test): notify the manager that the test finished.
            logger.debug("after_test for %s (success: %s)", test_case_name, not failed)
            cluster_status = await mgr.after_test(success=not failed)
            logger.info("Cluster after test %s (success: %s): %s", test_case_name, not failed, cluster_status)

        if cluster_status is not None and cluster_status["tasks_leaked"] and not failed:
            failed = True
            pytest.fail(
                f"test case {test_case_name} left unfinished tasks on the cluster manager: {cluster_status["message"]}"
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
                            decoded_bt = await decode_backtrace(mgr.cluster.mode, backtrace)
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
# used in tests to make schema changes.  Nothing is dropped afterwards: the
# cluster lives only as long as the test.
@pytest.fixture(scope="function")
async def random_tables(request, manager):
    rf_marker = request.node.get_closest_marker("replication_factor")
    replication_factor = rf_marker.args[0] if rf_marker is not None else 3  # Default 3
    enable_tablets = request.node.get_closest_marker("enable_tablets")
    enable_tablets = enable_tablets.args[0] if enable_tablets is not None else None
    yield RandomTables(request.node.name, manager, unique_name(),
                       replication_factor, None, enable_tablets)

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
async def storage(request, pytestconfig, tmpdir, suite_log_dir, manager: ScyllaClusterManager):
    """Parametrize tests over local / S3 / GCS storage.

    When storage is None the test runs with local (filesystem) storage.
    Otherwise the fixture yields an object-storage server handle.
    """
    if request.param is None:
        yield None
        return

    async with make_object_storage(request.param, pytestconfig, tmpdir, suite_log_dir, request.node.name, manager) as server:
        yield server
