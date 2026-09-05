#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# This file configures pytest for all tests in this directory, and also
# defines common test fixtures for all of them to use

import asyncio
import sys
import tempfile
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from cassandra.cluster import Session
from cassandra.connection import DRIVER_NAME, DRIVER_VERSION

from test import TOP_SRC_DIR, MODES_TIMEOUT_FACTOR, path_to
from test.pylib.async_cql import run_async
from test.pylib.connect_options import add_cql_connection_options, add_s3_options
from test.pylib.encryption_provider import KeyProvider, make_key_provider_factory
from test.pylib.object_storage import Storage, StorageFactory, StorageKind, create_gs_server, create_s3_server
from test.pylib.random_tables import RandomTables
from test.pylib.runner import PHASE_REPORT_KEY, make_failed_test_dir
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_scylla_2025_1_description
from test.pylib.skip_types import skip_env
from test.pylib.util import unique_name

SCRIPTS_DIR = str(TOP_SRC_DIR / "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable
    from typing import Any

    from test.pylib.scylla_cluster import ClusterFactory, ScyllaCluster


type TeardownCallback = Callable[[], Any]


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
async def scylla_cluster_teardowns() -> AsyncGenerator[list[TeardownCallback]]:
    """Cleanups to run once the test's cluster is gone.

    The scylla_cluster fixture depends on this one, so pytest runs these
    after the cluster's own teardown: a resource the servers use -- an
    object storage bucket, say -- must not be disposed of while they can
    still touch it (SCYLLADB-2471).  Fired in LIFO order; a failure is
    logged and swallowed so one cleanup cannot abort the rest.
    """
    teardowns = []
    yield teardowns
    for teardown in reversed(teardowns):
        try:
            result = teardown()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.warning("Cluster teardown %s failed",
                           getattr(teardown, "__qualname__", repr(teardown)), exc_info=True)


@pytest.fixture
async def scylla_cluster(request: pytest.FixtureRequest,
                         testpy_cluster_factory: ClusterFactory,
                         testpy_test_name: str,
                         scylla_cluster_teardowns: list[TeardownCallback]) -> AsyncGenerator[ScyllaCluster]:
    """A fresh empty ScyllaCluster for each test.

    Overrides the module-scoped fixture of the same name: every test in
    test/cluster gets its own cluster, disposed of by the factory.
    Depends on scylla_cluster_teardowns so those cleanups run once the
    cluster is gone.
    """
    async with testpy_cluster_factory(request.node, testpy_test_name) as cluster:
        yield cluster


@pytest.fixture
async def object_storage_factory(request: pytest.FixtureRequest,
                                 suite_log_dir: Path,
                                 scylla_cluster_teardowns: list[TeardownCallback]) -> StorageFactory:
    """A factory of object-storage backends for a test.

    The bucket is destroyed and the server stopped by the scylla_cluster teardowns, that is after the
    harness has disposed of the cluster.
    """
    async def make_object_storage(kind: StorageKind) -> Storage:
        match kind:
            case "gs":
                server = create_gs_server(suite_log_dir)
            case "s3":
                server = create_s3_server(request.config, request.getfixturevalue("tmpdir"), suite_log_dir)
            case _:
                raise RuntimeError(f"Unknown storage kind {kind}")

        await server.start()

        # Appended first so that it runs last: the bucket delete below needs the server.
        scylla_cluster_teardowns.append(server.stop)

        server.create_test_bucket(request.node.name)

        # Emptying the bucket while a node is still running can abort an in-flight
        # compaction, tablet migration or streaming operation (SCYLLADB-2471), so
        # it is deferred until the cluster is down.
        scylla_cluster_teardowns.append(server.destroy_test_bucket)

        return server
    return make_object_storage


@pytest.fixture
async def manager(request: pytest.FixtureRequest,
                  scylla_cluster: ScyllaCluster,
                  testpy_test_name: str) -> AsyncGenerator[ScyllaClusterManager]:
    """
    Per test cluster manager: connects the driver, and on teardown checks the
    server logs and reports the test's verdict to the manager.
    """
    test_case_name = request.node.name

    async with ScyllaClusterManager(
            test_name=testpy_test_name,
            cluster=scylla_cluster,
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

        yield mgr

        # `request.node.stash` contains reports stored per phase in `pytest_runtest_makereport`
        # from where we can retrieve test failure.
        reports = request.node.stash[PHASE_REPORT_KEY]
        call_report = reports.get("call")
        failed = call_report is not None and call_report.failed

        # Check if the test has the check_nodes_for_errors marker
        found_errors = await mgr.check_all_errors(
            all_errors=request.node.get_closest_marker("check_nodes_for_errors") is not None,
        )

        failed_test_dir_path = None
        if failed or found_errors:
            failed_test_dir_path = make_failed_test_dir(request.config, mgr.cluster.mode, test_case_name)
            # Gather the server logs while the manager is alive and the logs
            # still exist; the stacktrace and links are attached by
            # pytest_runtest_makereport.
            try:
                await mgr.gather_related_logs(failed_test_dir_path)
            except Exception:
                logger.warning("Failed to gather logs for failed test %s", test_case_name, exc_info=True)

        # Close the driver before after_test(): nothing in after_test() needs
        # it.  after_test() runs even if closing fails: it reports what the
        # test left behind.
        try:
            mgr.driver_close()
        finally:
            # Tear down (after test): notify the manager that the test finished.
            logger.debug("after_test for %s (success: %s)", test_case_name, not failed)
            cluster_status = await mgr.after_test(success=not failed)
            logger.info("Cluster after test %s (success: %s): %s", test_case_name, not failed, cluster_status)

        # Collect the teardown-detected failures and report them all at once,
        # so leaked tasks don't hide the found-errors report or vice versa.
        teardown_failures = []
        if cluster_status is not None and cluster_status["tasks_leaked"]:
            teardown_failures.append(
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
            teardown_failures.extend(full_message)

        if teardown_failures:
            if failed_test_dir_path is None:
                # A leaked-tasks-only failure surfaces here, after the gather
                # above; copy the logs now, before recycle() deletes them.
                failed_test_dir_path = make_failed_test_dir(request.config, mgr.cluster.mode, test_case_name)
                try:
                    await mgr.gather_related_logs(failed_test_dir_path)
                except Exception:
                    logger.warning("Failed to gather logs for failed test %s", test_case_name, exc_info=True)
            if not failed:
                pytest.fail(f"\n{'\n'.join(teardown_failures)}")


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


@pytest.fixture(params=[None, "s3", "gs"], ids=["local", "s3", "gs"])
async def storage(request: pytest.FixtureRequest, object_storage_factory: StorageFactory) -> Storage | None:
    """Parametrize tests over local / S3 / GCS storage.

    When storage is None the test runs with local (filesystem) storage.
    Otherwise, the fixture returns an object-storage server handle.
    """
    if request.param is None:
        return None
    return await object_storage_factory(request.param)
