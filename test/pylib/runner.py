#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import platform
import random
import shutil
import sys
import time
from argparse import BooleanOptionalAction
from collections import defaultdict
from itertools import chain, count
from functools import cache, cached_property
from random import randint
from typing import TYPE_CHECKING, Callable
from types import SimpleNamespace

import pytest
import universalasync
import xdist
import yaml
from _pytest.junitxml import xml_key


from test import ALL_MODES, DEBUG_MODES, TOP_SRC_DIR, HOST_ID, path_to
from test.pylib.artifact_registry import ArtifactRegistry as artifacts
from test.pylib.ldap_server import start_ldap
from test.pylib.minio_server import MinioServer
from test.pylib.pool import Pool
from test.pylib.resource_gather import setup_cgroup, setup_worker_cgroup, get_resource_gather, SystemResourceMonitor, \
    SCYLLA_TEST_CGROUP_BASE_ENV, gather_host_info
from test.pylib.db.writer import SQLiteWriter, DEFAULT_DB_NAME, HOST_INFO_TABLE
from test.pylib.host_registry import HostRegistry
from test.pylib.s3_proxy import S3ProxyServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.scylla_cluster import (
    ScyllaCluster,
    ScyllaServer,
    get_current_version_description,
    merge_cmdline_options,
)
from test.pylib.skip_reason_plugin import skip_marker
from test.pylib.util import get_modes_to_run, scale_timeout_by_mode, get_xdist_worker_id, LogPrefixAdapter
from test.pylib.version_fetch_utils import fetch_and_install_scylla_version

if TYPE_CHECKING:
    import argparse
    from collections.abc import Generator, AsyncGenerator

    import _pytest.nodes
    import _pytest.scope


TEST_CONFIG_FILENAME = "test_config.yaml"
PYTEST_LOG_FOLDER = "pytest_log"
PYTEST_TESTS_LOGS_FOLDER = "pytest_tests_logs"

REPEATING_FILES = pytest.StashKey[set[pathlib.Path]]()
BUILD_MODE = pytest.StashKey[str]()
RUN_ID = pytest.StashKey[int]()
PYTEST_LOG_FILE = pytest.StashKey[str]()

EXIT_MAXFAIL_REACHED = 11

logger = logging.getLogger(__name__)

# Store pytest config globally so we can access it in hooks that only receive report
_pytest_config: pytest.Config | None = None

_system_resource_monitor: SystemResourceMonitor | None = None

_cluster_pools: dict[str, Pool[ScyllaCluster]] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption('--mode', choices=ALL_MODES, action="append", dest="modes",
                     help="Run only tests for given build mode(s)")
    parser.addoption('--tmpdir', action='store', default=str(TOP_SRC_DIR / 'testlog'),
                     help='Path to temporary test data and log files.  The data is further segregated per build mode.')
    parser.addoption('--run_id', action='store', default=None, help='Run id for the test run')
    parser.addoption('--byte-limit', action="store", default=randint(0, 2000), type=int,
                     help="Specific byte limit for failure injection (random by default)")
    parser.addoption("--gather-metrics", action=BooleanOptionalAction, default=False,
                     help='Switch on gathering cgroup metrics')
    parser.addoption('--random-seed', action="store",
                     help="Random number generator seed to be used by boost tests")

    # Following option is to use with bare pytest command.
    #
    # For compatibility with reasons need to run bare pytest with  --test-py-init option
    # to run a test.py-compatible pytest session.
    #
    # TODO: remove this when we'll completely switch to bare pytest runner.
    parser.addoption('--test-py-init', action='store_true', default=False, deprecated=True,
                     help='Run pytest session in test.py-compatible mode.  I.e., start all required services, etc.')

    # Options for compatibility with test.py
    parser.addoption('--save-log-on-success', default=False,
                     dest="save_log_on_success", action="store_true",
                     help="Save test log output on success and skip cleanup before the run.")
    parser.addoption('--coverage', action='store_true', default=False,
                     help="When running code instrumented with coverage support"
                          "Will route the profiles to `tmpdir`/mode/coverage/`suite` and post process them in order to generate "
                          "lcov file per suite, lcov file per mode, and an lcov file for the entire run, "
                          "The lcov files can eventually be used for generating coverage reports")
    parser.addoption("--coverage-mode", action='append', type=str, dest="coverage_modes",
                     help="Collect and process coverage only for the modes specified. implies: --coverage, default: All built modes")
    parser.addoption("--cluster-pool-size", type=int,
                     help="Set the pool_size for the per-suite Scylla cluster pool.  Alternatively environment variable "
                          "CLUSTER_POOL_SIZE can be used to achieve the same")
    parser.addoption("--extra-scylla-cmdline-options", default='',
                     help="Passing extra scylla cmdline options for all tests.  Options should be space separated:"
                          " '--logger-log-level raft=trace --default-log-level error'")
    parser.addoption('--x-log2-compaction-groups', action="store", default="0", type=int,
                     help="Controls number of compaction groups to be used by Scylla tests. Value of 3 implies 8 groups.")
    parser.addoption('--repeat', action="store", default=1, type=int,
                     help="number of times to repeat test execution")

    parser.addoption('--exe-path', default=False,
                     dest="exe_path", action="store",
                     help="Path to the executable to run. Not working with `mode`")
    parser.addoption('--exe-url', default=False,
                     dest="exe_url", action="store",
                     help="URL to download the relocatable executable. Not working with `mode`")

# Stores the per-phase test reports so that fixtures and hooks can inspect the
# outcome of each phase (setup / call / teardown) independently.
PHASE_REPORT_KEY = pytest.StashKey[dict[str, pytest.CollectReport]]()


def _build_test_mock(item: pytest.Item) -> SimpleNamespace:
    """Build a SimpleNamespace test object for resource gathering from any pytest item.

    Works for both Python test items and C++ CppTestCase items, providing a
    unified interface for the resource-gather subsystem.
    """
    from test.pylib.cpp.base import CppTestCase

    params_stash = get_params_stash(node=item)
    build_mode = params_stash[BUILD_MODE] if params_stash else item.config.build_modes[0]
    run_id = item.stash.get(RUN_ID, None) or item.config.getoption("--run_id")
    temp_dir = pathlib.Path(item.config.getoption("--tmpdir")).absolute()

    # Strip the ".mode.run_id" suffix appended by modify_pytest_item()
    test_name = item.name
    suffix = f".{build_mode}.{run_id}"
    original_test_name = test_name[:-len(suffix)] if test_name.endswith(suffix) else test_name

    file_path = item.path
    suite_path = file_path.parent

    if isinstance(item, CppTestCase):
        file_name = f"{item.parent.test_name}.cc"
        shortname = item.test_case_name
    else:
        file_name = file_path.name
        shortname = original_test_name

    return SimpleNamespace(
        time_end=0,
        time_start=0,
        id=run_id,
        mode=build_mode,
        success=False,
        status=None,
        path=file_path,
        shortname=shortname,
        suite=SimpleNamespace(
            log_dir=temp_dir / build_mode,
            name=suite_path.name,
            suite_path=suite_path,
            test_file_name=file_name,
        ),
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(item, nextitem):
    test_mock = _build_test_mock(item)
    test_mock.time_start = time.time()

    resource_gather = get_resource_gather(
        temp_dir=pathlib.Path(item.config.getoption("--tmpdir")),
        is_switched_on=item.config.getoption("--gather-metrics"),
        test=test_mock,
        worker_id=os.environ.get("PYTEST_XDIST_WORKER"),
    )
    try:
        resource_gather.setup_test_tracking()
        resource_gather.cgroup_monitor()
    except Exception:
        resource_gather.stop_monitoring()
        resource_gather.teardown_test_tracking()
        raise
    try:
        return (yield)
    finally:
        if resource_gather is not None:
            test_mock.time_end = time.time()
            resource_gather.stop_monitoring()

            try:
                reports = item.stash.get(PHASE_REPORT_KEY, {})
                # skipped test have no call report so need to get setup report instead
                call_report = reports.get("call") if reports.get("call") is not None else reports.get("setup")
                success = call_report is not None and not call_report.failed
                test_metrics = resource_gather.get_test_metrics()
                if call_report is not None:
                    status = "skipped" if call_report.skipped else call_report.outcome
                    if hasattr(call_report, "wasxfail"):
                        if call_report.skipped:
                            status = "xfailed"
                        elif call_report.passed:
                            status = "xpassed"
                    else:
                        # with xfail_strict = true wasxfail is not present when test is xpassed, so need to check report
                        if 'XPASS' in call_report.longreprtext:
                            status = "xpassed"
                else:
                    status = "unknown"
                test_metrics.status = status

                resource_gather.write_metrics_to_db(
                    metrics=test_metrics,
                    success=success
                )
            finally:
                resource_gather.teardown_test_tracking()


@pytest.fixture(scope="module", autouse=True)
def build_mode(request: pytest.FixtureRequest) -> str:
    params_stash = get_params_stash(node=request.node)
    if params_stash is None:
        return request.config.build_modes[0]
    return params_stash[BUILD_MODE]


@pytest.fixture(scope="module")
def testpy_shortname(request: pytest.FixtureRequest) -> str:
    return str(request.path.relative_to(get_params_stash(node=request.node)[TEST_SUITE].path).with_suffix(""))


@pytest.fixture(scope="module")
def testpy_uname(request: pytest.FixtureRequest, testpy_shortname: str) -> str:
    params_stash = get_params_stash(node=request.node)
    uname = f"{params_stash[TEST_SUITE].name}.{testpy_shortname.replace("/", "_")}.{params_stash[RUN_ID]}"
    if worker_id := get_xdist_worker_id():
        uname = f"{worker_id}.{uname}"
    return uname


@pytest.fixture(scope="module")
def scale_timeout(build_mode: str) -> Callable[[int | float], int | float]:
    def scale_timeout_inner(timeout: int | float) -> int | float:
        return scale_timeout_by_mode(build_mode, timeout)

    return scale_timeout_inner


@pytest.fixture(scope="module")
def testpy_suite_clusters(request: pytest.FixtureRequest,
                          build_mode: str,
                          suite_log_dir: pathlib.Path,
                          scylla_binary: str) -> Pool[ScyllaCluster]:
    suite_config = get_params_stash(node=request.node)[TEST_SUITE]
    suite_key = os.path.join(suite_config.path, build_mode)
    pool = _cluster_pools.get(suite_key)
    if not pool:
        _cluster_pools[suite_key] = pool = create_suite_pool(
            suite_config=suite_config,
            options=request.config.option,
            mode=build_mode,
            log_dir=suite_log_dir,
            scylla_exe=scylla_binary,
        )
    return pool


@pytest.fixture(scope="module")
def suite_log_dir(request: pytest.FixtureRequest, build_mode: str) -> pathlib.Path:
    return pathlib.Path(request.config.getoption("--tmpdir")).joinpath(build_mode).absolute()


@pytest.fixture(scope="module")
def scylla_binary(request: pytest.FixtureRequest, build_mode: str) -> str:
    return request.config.getoption("--exe-path") or path_to(build_mode, "scylla")


@pytest.fixture(scope="module")
async def scylla_cluster(request: pytest.FixtureRequest,
                         testpy_suite_clusters: Pool[ScyllaCluster],
                         build_mode: str,
                         testpy_shortname: str,
                         testpy_uname: str) -> AsyncGenerator[ScyllaCluster]:
    """Lease a ScyllaCluster from the pool for the tests in a module.

    Gets a cluster from the suite's pool, runs the before-test hook and
    yields it to the module's tests. Once the module is done the cluster is
    always returned to the pool marked dirty, so it is recycled rather than
    reused by a later module.
    """
    logger_prefix = f"{build_mode}/"
    cluster_logger = LogPrefixAdapter(logging.getLogger(logger_prefix), {"prefix": logger_prefix})
    cluster: ScyllaCluster | None = None
    server_log_filename: pathlib.Path | None = None
    testpy_name = os.path.join(get_params_stash(node=request.node)[TEST_SUITE].name, testpy_shortname.split('.')[0])
    is_before_test_ok = False
    is_after_test_ok = False
    try:
        cluster: ScyllaCluster = await testpy_suite_clusters.get(cluster_logger)
        cluster.before_test(testpy_uname)
        cluster_logger.info("Leasing Scylla cluster %s for test %s", cluster, testpy_uname)
        server_log_filename = cluster.server_log_filename()
        is_before_test_ok = True
        cluster.take_log_savepoint()

        yield cluster

        cluster.after_test(testpy_uname)
        is_after_test_ok = True
    except Exception as exc:
        if not is_before_test_ok:
            logger.info("Test %s pre-check failed: %s\ncheck server logs: %s", testpy_name, exc, server_log_filename)
            cluster_logger.info("Discarding cluster after failed start for test %s...", testpy_name)
        elif not is_after_test_ok:
            logger.info("Test %s post-check failed: %s\ncheck server logs: %s", testpy_name, exc, server_log_filename)
            cluster_logger.info("Discarding cluster after failed test %s...", testpy_name)
        raise
    finally:
        if cluster is not None:
            cluster_logger.info("Test %s finished", testpy_uname)
            await testpy_suite_clusters.put(cluster, is_dirty=True)


def pytest_collection_modifyitems(items: list[pytest.Item], config: pytest.Config) -> None:
    run_ids = defaultdict(lambda: count(start=int(config.getoption("--run_id") or 1)))
    for item in items:
        modify_pytest_item(item=item, run_ids=run_ids)

    suites_order = defaultdict(count().__next__)  # number suites in order of appearance

    def sort_key(item: pytest.Item) -> tuple[int, bool]:
        suite = item.stash[TEST_SUITE]
        return suites_order[suite], suite and item.path.stem not in suite.cfg.get("run_first", [])

    items.sort(key=sort_key)


def pytest_sessionstart(session: pytest.Session) -> None:
    if session.config.getoption("--collect-only"):
        return

    artifacts.init()
    artifacts.add_exit_artifact(HostRegistry().cleanup)

    # Check if this is an xdist worker
    is_xdist_worker = xdist.is_xdist_worker(request_or_session=session)

    gather_metrics = session.config.getoption("--gather-metrics")
    temp_dir = pathlib.Path(session.config.getoption("--tmpdir")).absolute()

    # Run stuff just once for the main pytest process (not in xdist workers).
    if not is_xdist_worker:
        prepare_environment(
            tempdir_base=temp_dir,
            modes=get_modes_to_run(session.config),
            gather_metrics=gather_metrics,
            save_log_on_success=session.config.getoption("--save-log-on-success"),
            toxiproxy_byte_limit= session.config.getoption("--byte-limit"),
        )
    if gather_metrics:
        # In the master process, set up the cgroup hierarchy if test.py hasn't done it already.
        # Workers inherit SCYLLA_TEST_CGROUP_BASE_ENV from the master via environment inheritance.
        if not is_xdist_worker and SCYLLA_TEST_CGROUP_BASE_ENV not in os.environ:
            setup_cgroup(is_required=True)
        setup_worker_cgroup()
        # System-wide resource metrics (CPU%, memory) are identical from any process.
        # Only the master needs to record them.
        if not is_xdist_worker:
            global _system_resource_monitor
            _system_resource_monitor = SystemResourceMonitor(temp_dir)
            _system_resource_monitor.start()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report):
    """Add custom XML attributes to JUnit testcase elements.

    This hook wraps the node_reporter's to_xml method to add custom attributes
    when the XML element is created. This approach works with pytest-xdist because
    it modifies the XML element directly when it's generated, rather than trying
    to modify attrs before finalize() is called.

    Attributes added:
    - function_path: The function path of the test case (excluding parameters).

    Uses tryfirst=True to run before LogXML's hook has created the node_reporter to avoid double recording.
    """
    # Get the XML reporter
    config = _pytest_config
    if config is None:
        return

    xml = config.stash.get(xml_key, None)
    if xml is None:
        return

    node_reporter = xml.node_reporter(report)

    # Only wrap once to avoid multiple wrapping (check on the node_reporter object itself)
    if not getattr(node_reporter, '__reporter_modified', False):

        function_path = f'test/{report.nodeid.rsplit('.', 2)[0].rsplit('[', 1)[0]}'

        # Wrap the to_xml method to add custom attributes to the element
        original_to_xml = node_reporter.to_xml

        def custom_to_xml():
            """Wrapper that adds custom attributes to the testcase element."""
            element = original_to_xml()
            element.set("function_path", function_path)
            return element

        node_reporter.to_xml = custom_to_xml
        node_reporter.__reporter_modified = True


def pytest_sessionfinish(session: pytest.Session) -> None:
    if session.config.getoption("--collect-only"):
        return

    global _system_resource_monitor
    if _system_resource_monitor:
        _system_resource_monitor.stop()

    is_xdist_worker = xdist.is_xdist_worker(request_or_session=session)
    # If all tests passed, remove the log file to save space and avoid confusion with logs from failed runs.
    # We check this at the end of the session to ensure that we have the complete log available for any failed tests.

    if session.testsfailed == 0 and not session.config.getoption("--save-log-on-success"):
        # Use missing_ok=True because the log file is only created on first write,
        # so it may never have been written if nothing was logged.
        pathlib.Path(_pytest_config.stash[PYTEST_LOG_FILE]).unlink(missing_ok=True)


    # Check if this is an xdist worker - workers should not clean up (only the main process should)
    # Check if test.py has already prepared the environment, so it should clean up

    if is_xdist_worker:
        return

    asyncio.run(artifacts.cleanup_before_exit())

    # Modify exit code to reflect the number of failed tests for easier detection in CI.
    maxfail = session.config.getoption("maxfail")

    if 0 < maxfail <= session.testsfailed:
        session.exitstatus = EXIT_MAXFAIL_REACHED


def pytest_configure(config: pytest.Config) -> None:
    global _pytest_config
    _pytest_config = config
    log_file_format = config.getini("log_file_format") or config.getini("log_format") or "%(asctime)s %(levelname)s %(name)s> %(message)s"
    log_file_level = config.getini("log_file_level") or config.getini("log_level") or "INFO"

    pytest_log_dir = pathlib.Path(_pytest_config.getoption("--tmpdir")).absolute() / PYTEST_LOG_FOLDER
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    # If this is an xdist worker, set up logging to a separate file for this worker. Otherwise, set up logging for the main process.
    if worker_id is not None:
        _pytest_config.stash[PYTEST_LOG_FILE] = f"{pytest_log_dir}/pytest_{worker_id}_{HOST_ID}.log"
    else:
        # For the main process, we want to clean up old logs before the run, so we create the log directory and remove any existing log files.
        pytest_log_dir.mkdir(parents=True, exist_ok=True)
        if not _pytest_config.getoption("--save-log-on-success"):
            for file in pytest_log_dir.glob("*"):
                # This will help in case framework tests are executed with test.py event if it's the wrong way to run them.
                # test_no_bare_skip_markers_in_collection uses a subprocess to run a collection that has lead to race
                # condition, especially with repeat.
                file.unlink(missing_ok=True)

        _pytest_config.stash[PYTEST_LOG_FILE] = f"{pytest_log_dir}/pytest_main_{HOST_ID}.log"

    # Explicitly configure the root logger to write exclusively to a file.
    # logging.basicConfig() is a no-op when the root logger already has handlers
    # (e.g. added by pytest or any early import), which would leave a StreamHandler
    # in place and cause all log records — including noisy third-party DEBUG messages
    # like urllib3.connectionpool or asyncio — to appear on the terminal.
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    file_handler = logging.FileHandler(_pytest_config.stash[PYTEST_LOG_FILE])
    file_handler.setFormatter(logging.Formatter(log_file_format))
    root_logger.addHandler(file_handler)
    root_logger.setLevel(log_file_level)

    if exe_url := config.getoption("--exe-url"):
        if config.getoption("--exe-path"):
            raise RuntimeError("Can't use --exe-url and exe-path simultaneously.")
        config.option.exe_path = fetch_and_install_scylla_version(url=exe_url)

    if config.getoption("--exe-path"):
        if config.getoption("--mode"):
            raise RuntimeError("Can't use --mode with --exe-path or --exe-url.")
        config.option.modes = ["custom_exe"]

    os.environ["TOPOLOGY_RANDOM_FAILURES_TEST_SHUFFLE_SEED"] = os.environ.get("TOPOLOGY_RANDOM_FAILURES_TEST_SHUFFLE_SEED", str(random.randint(0, sys.maxsize)))
    config.build_modes = get_modes_to_run(config)

    if testpy_run_id := config.getoption("--run_id"):
        if config.getoption("--repeat") != 1:
            raise RuntimeError("Can't use --run_id and --repeat simultaneously.")
    # Write host hardware info once, at the very start, before any test preparation.
    # Done unconditionally (not gated on --gather-metrics) so that the host_info FK
    # referenced by every other table is always populated, regardless of whether
    # full metrics collection is enabled.
    # Only in the main process — xdist workers share the same DB and the same host_id,
    # so there is nothing new to record.
    if os.environ.get("PYTEST_XDIST_WORKER") is None and not config.getoption("--collect-only"):
        temp_dir = pathlib.Path(config.getoption("--tmpdir")).absolute()
        writer = SQLiteWriter(temp_dir / DEFAULT_DB_NAME)
        try:
            writer.write_row_if_not_exist(gather_host_info(), HOST_INFO_TABLE, id_column="host_id")
        finally:
            writer.close()


class DisabledFile(pytest.File):
    def collect(self) -> list[pytest.Item]:
        pytest.skip("All tests in this file are disabled in requested modes according to the suite config.")


@pytest.hookimpl(wrapper=True)
def pytest_collect_file(file_path: pathlib.Path,
                        parent: pytest.Collector) -> Generator[None, list[pytest.Collector], list[pytest.Collector]]:
    collectors = yield

    if len(collectors) == 1 and file_path not in parent.stash.setdefault(REPEATING_FILES, set()):
        parent.stash[REPEATING_FILES].add(file_path)

        build_modes = parent.config.build_modes
        if suite_config := TestSuiteConfig.from_pytest_node(node=collectors[0]):
            build_modes = (
                mode for mode in build_modes
                if not suite_config.is_test_disabled(build_mode=mode, path=file_path)
            )
        if repeats := [mode for mode in build_modes for _ in range(parent.config.getoption("--repeat"))]:
            ihook = parent.ihook
            collectors = list(chain(collectors, chain.from_iterable(
                ihook.pytest_collect_file(file_path=file_path, parent=parent) for _ in range(1, len(repeats))
            )))
            for build_mode, collector in zip(repeats, collectors, strict=True):
                collector.stash[BUILD_MODE] = build_mode
                collector.stash[TEST_SUITE] = suite_config
        else:
            collectors = [DisabledFile.from_parent(parent=parent, path=file_path)]

        parent.stash[REPEATING_FILES].remove(file_path)

    return collectors


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Post-test hook to store test result in stash and optionally save logs.

    Stores each phase's report in item.stash[PHASE_REPORT_KEY][phase] so
    fixtures and hooks can access the test outcome per phase.  `item.stash`
    is the same stash as `request.node.stash` in pytest fixtures.

    When --test-py-init is set, also saves failed test details to log files.
    """
    outcome = yield
    report = outcome.get_result()

    # Store report per phase for use by fixtures and hooks
    item.stash.setdefault(PHASE_REPORT_KEY, {})[report.when] = report

    # Optionally save test failure logs to files
    if _pytest_config:
        pytest_tests_logs = pathlib.Path(_pytest_config.getoption("--tmpdir")).absolute() / PYTEST_TESTS_LOGS_FOLDER
        if report.failed or _pytest_config.getoption("--save-log-on-success"):
            with open(pytest_tests_logs / f"{item._nodeid.replace('::', '-').replace('/', '-')}-{report.when}-{HOST_ID}.log", 'a') as f:
                f.write(report.longreprtext + "\n")
                for section in report.sections:
                    f.write(section[0] + "\n")
                    f.write(section[1] + "\n")

class TestSuiteConfig:
    def __init__(self, config_file: pathlib.Path):
        self.path = config_file.parent
        self.cfg = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}

    @cached_property
    def name(self) -> str:
        return self.path.name

    @cached_property
    def _run_in_specific_mode(self) -> set[str]:
        return set(chain.from_iterable(self.cfg.get(f"run_in_{build_mode}", []) for build_mode in ALL_MODES))

    @cache
    def disabled_tests(self, build_mode: str) -> set[str]:
        result = set(self.cfg.get("disable", []))
        result.update(self.cfg.get(f"skip_in_{build_mode}", []))
        if build_mode in DEBUG_MODES:
            result.update(self.cfg.get("skip_in_debug_modes", []))
        run_in_this_mode = set(self.cfg.get(f"run_in_{build_mode}", []))
        result.update(self._run_in_specific_mode - run_in_this_mode)
        return result

    def is_test_disabled(self, build_mode: str, path: pathlib.Path) -> bool:
        return str(path.relative_to(self.path).with_suffix("")) in self.disabled_tests(build_mode=build_mode)

    @classmethod
    def from_pytest_node(cls, node: _pytest.nodes.Node) -> TestSuiteConfig | None:
        config_file = node.path / TEST_CONFIG_FILENAME
        if config_file.is_file():
            suite = cls(config_file=config_file)
        else:
            if node.parent is None:
                return None
            suite = node.parent.stash.get(TEST_SUITE, None)
            if suite is None:
                suite = cls.from_pytest_node(node=node.parent)
        if suite:
            extra_opts = node.config.getoption("--extra-scylla-cmdline-options")
            if extra_opts:
                extra_cmd = suite.cfg.get('extra_scylla_cmdline_options', [])
                extra_cmd = merge_cmdline_options(extra_cmd, extra_opts.split())
                suite.cfg['extra_scylla_cmdline_options'] = extra_cmd
            node.stash[TEST_SUITE] = suite
        return suite


TEST_SUITE = pytest.StashKey[TestSuiteConfig | None]()

_STASH_KEYS_TO_COPY = BUILD_MODE, TEST_SUITE, RUN_ID


def get_params_stash(node: _pytest.nodes.Node) -> pytest.Stash | None:
    parent = node.getparent(cls=pytest.File)
    if parent is None:
        return None
    return parent.stash


def modify_pytest_item(item: pytest.Item, run_ids: defaultdict[tuple[str, str], count]) -> None:
    params_stash = get_params_stash(node=item)

    if RUN_ID not in params_stash:
        params_stash[RUN_ID] = next(run_ids[(params_stash[BUILD_MODE], item.parent._nodeid)])
    for key in _STASH_KEYS_TO_COPY:
        item.stash[key] = params_stash[key]

    suffix = f".{item.stash[BUILD_MODE]}.{item.stash[RUN_ID]}"

    item._nodeid = f"{item._nodeid}{suffix}"
    item.name = f"{item.name}{suffix}"
    skip_marks = [
        mark for mark in item.iter_markers("skip_mode")
        if mark.name == "skip_mode"
    ]

    for mark in skip_marks:
        def __skip_test(mode, reason, platform_key=None):
            modes = [mode] if isinstance(mode, str) else mode

            for mode in modes:
                if mode == item.stash[BUILD_MODE]:
                    if platform_key is None or platform_key in platform.platform():
                        skip_marker(item, reason, skip_type="mode")
        try:
            __skip_test(*mark.args, **mark.kwargs)
        except TypeError as e:
            raise TypeError(f"Failed to process skip_mode mark, {mark} for test {item}, error {e}")

    if (any(mark.name == "xfail" for mark in item.iter_markers("xfail"))
            and not any(mark.name == "nightly" for mark in item.iter_markers("nightly"))):
        item.add_marker(pytest.mark.nightly)

    if (any(mark.name in ("perf", "manual", "unstable") for mark in item.iter_markers())
            and not any(mark.name == "non_gating" for mark in item.iter_markers("non_gating"))):
        item.add_marker(pytest.mark.non_gating)


def prepare_dir(dirname: pathlib.Path, pattern: str, save_log_on_success: bool) -> None:
    # Ensure the dir exists.
    dirname.mkdir(parents=True, exist_ok=True)

    if not save_log_on_success:
        # Remove old artifacts.
        if pattern == '*':
            shutil.rmtree(dirname, ignore_errors=True)
        else:
            for p in dirname.glob(pattern):
                p.unlink()


def prepare_dirs(tempdir_base: pathlib.Path,
                 modes: list[str],
                 gather_metrics: bool,
                 save_log_on_success: bool = False) -> None:
    setup_cgroup(gather_metrics)
    prepare_dir(tempdir_base, "*.log", save_log_on_success)
    prepare_dir(tempdir_base/ PYTEST_TESTS_LOGS_FOLDER, "*.log", save_log_on_success)
    for directory in ['report', 'ldap_instances']:
        full_path_directory = tempdir_base / directory
        prepare_dir(full_path_directory, '*', save_log_on_success)
    for mode in modes:
        prepare_dir(tempdir_base / mode, "*.log", save_log_on_success)
        prepare_dir(tempdir_base / mode, "*.reject", save_log_on_success)
        prepare_dir(tempdir_base / mode / "xml", "*.xml", save_log_on_success)
        prepare_dir(tempdir_base / mode / "failed_test", "*", save_log_on_success)
        prepare_dir(tempdir_base / mode / "allure", "*.xml", save_log_on_success)


def _make_service_logger(logger_name: str, log_file: pathlib.Path) -> logging.Logger:
    """Return a logger that writes exclusively to *log_file*.

    Disables propagation to the root logger so service start/stop messages
    never appear on the console.
    """
    svc_logger = logging.getLogger(logger_name)
    svc_logger.propagate = False
    svc_logger.setLevel(logging.DEBUG)
    if not svc_logger.handlers:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        svc_logger.addHandler(handler)
    return svc_logger


@universalasync.async_to_sync_wraps
async def start_3rd_party_services(tempdir_base: pathlib.Path, toxiproxy_byte_limit: int):
    hosts = HostRegistry()

    finalize = start_ldap(
        host=await hosts.lease_host(),
        port=5000,
        instance_root=tempdir_base / 'ldap_instances',
        toxiproxy_byte_limit=toxiproxy_byte_limit)
    async def make_async_finalize():
        finalize()

    artifacts.add_exit_artifact(make_async_finalize)
    ms = MinioServer(
        tempdir_base=str(tempdir_base),
        address=await hosts.lease_host(),
        logger=LogPrefixAdapter(
            logger=_make_service_logger("minio", tempdir_base / "minio.log"),
            extra={"prefix": "minio"},
        ),
    )
    await ms.start()
    artifacts.add_exit_artifact(ms.stop)

    mock_s3_server = MockS3Server(
        host=await hosts.lease_host(),
        port=2012,
        logger=LogPrefixAdapter(
            logger=_make_service_logger("s3_mock", tempdir_base / "s3_mock.log"),
            extra={"prefix": "s3_mock"},
        ),
    )
    await mock_s3_server.start()
    artifacts.add_exit_artifact(mock_s3_server.stop)

    minio_uri = f"http://{os.environ[ms.ENV_ADDRESS]}:{os.environ[ms.ENV_PORT]}"
    proxy_s3_server = S3ProxyServer(
        host=await hosts.lease_host(),
        port=9002,
        minio_uri=minio_uri,
        max_retries=3,
        seed=int(time.time()),
        logger=LogPrefixAdapter(
            logger=_make_service_logger("s3_proxy", tempdir_base / "s3_proxy.log"),
            extra={"prefix": "s3_proxy"},
        ),
    )
    await proxy_s3_server.start()
    artifacts.add_exit_artifact(proxy_s3_server.stop)


def prepare_environment(tempdir_base: pathlib.Path,
                        modes: list[str],
                        gather_metrics: bool,
                        save_log_on_success: bool,
                        toxiproxy_byte_limit: int) -> None:
    prepare_dirs(tempdir_base, modes, gather_metrics, save_log_on_success=save_log_on_success)
    start_3rd_party_services(tempdir_base=tempdir_base, toxiproxy_byte_limit=toxiproxy_byte_limit)


def create_suite_pool(suite_config: TestSuiteConfig,
                      options: argparse.Namespace,
                      mode: str,
                      log_dir: pathlib.Path,
                      scylla_exe: str) -> Pool[ScyllaCluster]:
    # environment variables that should be the base of all processes running in this suit
    base_env = {}

    if options.coverage and (mode in options.coverage_modes) and bool(suite_config.cfg.get("coverage", True)):
        # Set the coverage data from each instrumented object to use the same file (and merged into it with locking)
        # as long as we don't need test specific coverage data, this looks sufficient. The benefit of doing this in
        # this way is that the storage will not be bloated with coverage files (each can weigh 10s of MBs so for several
        # thousands of tests it can easily reach 10 of GBs)
        # ref: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html#running-the-instrumented-program
        base_env["LLVM_PROFILE_FILE"] = str(log_dir / "coverage" / suite_config.name / "%m.profraw")

    cluster_size = suite_config.cfg.get("cluster", {}).get("initial_size", 1)
    env_pool_size = os.getenv("CLUSTER_POOL_SIZE")
    if options.cluster_pool_size is not None:
        pool_size = options.cluster_pool_size
    elif env_pool_size is not None:
        pool_size = int(env_pool_size)
    else:
        pool_size = suite_config.cfg.get("pool_size", 2)

    def create_server(create_cfg: ScyllaCluster.CreateServerParams):
        cmdline_options = suite_config.cfg.get("extra_scylla_cmdline_options", [])
        if type(cmdline_options) == str:
            cmdline_options = [cmdline_options]
        cmdline_options = merge_cmdline_options(cmdline_options, create_cfg.cmdline_from_test)
        cmdline_options = merge_cmdline_options(cmdline_options, options.extra_scylla_cmdline_options.split())
        # There are multiple sources of config options, with increasing priority
        # (if two sources provide the same config option, the higher priority one wins):
        # 1. the defaults
        # 2. suite-specific config options (in "extra_scylla_config_options")
        # 3. config options from tests (when servers are added during a test)
        config_options = {
            "authenticator": "PasswordAuthenticator",
            "authorizer": "CassandraAuthorizer",
            "tablets_initial_scale_factor": 4 if mode == "release" else 2,
            **suite_config.cfg.get("extra_scylla_config_options", {}),
            **create_cfg.config_from_test,
        }
        server = ScyllaServer(
            mode=mode,
            version=(create_cfg.version or get_current_version_description(scylla_exe)),
            vardir=log_dir,
            logger=create_cfg.logger,
            cluster_name=create_cfg.cluster_name,
            ip_addr=create_cfg.ip_addr,
            seeds=create_cfg.seeds,
            cmdline_options=cmdline_options,
            config_options=config_options,
            property_file=create_cfg.property_file,
            append_env=base_env,
            server_encryption=create_cfg.server_encryption)

        return server

    async def create_cluster(logger: logging.Logger | logging.LoggerAdapter) -> ScyllaCluster:
        cluster = ScyllaCluster(logger, cluster_size, create_server)

        async def stop() -> None:
            await cluster.stop()

        artifacts.add_exit_artifact(stop)

        if not options.save_log_on_success:
            # If a test fails, we might want to keep the data dirs.
            async def uninstall() -> None:
                await cluster.uninstall()

            artifacts.add_exit_artifact(uninstall)

        await cluster.install_and_start()
        # If cluster failed to start, raise the exception immediately
        # so the pool doesn't return a broken cluster to tests
        if cluster.start_exception is not None:
            # Clean up the broken cluster before raising
            try:
                await cluster.stop()
                if cluster.api is not None:
                    cluster.api.close()
                    cluster.api = None
                await cluster.release_ips()
            except:
                pass  # Ignore cleanup errors
            raise cluster.start_exception
        return cluster

    async def recycle_cluster(cluster: ScyllaCluster) -> None:
        """When a dirty cluster is returned to the cluster pool,
           stop it and release the used IPs. We don't necessarily uninstall() it yet,
           which would delete the log file and directory - we might want to preserve
           these if it came from a failed test.
        """
        await cluster.stop()
        for srv in cluster.servers.values():
            if srv.log_file is not None:
                srv.log_file.close()
            srv.maintenance_socket_dir.cleanup()
        # Close API client to release connector resources
        if cluster.api is not None:
            cluster.api.close()
            cluster.api = None
        await cluster.release_ips()

    return Pool(pool_size, create_cluster, recycle_cluster)
