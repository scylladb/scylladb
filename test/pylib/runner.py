#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import platform
import random
import sys
import time
from argparse import BooleanOptionalAction
from collections import defaultdict
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import chain, count, product
from functools import cache, cached_property
from random import randint
from threading import Event
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
import xdist
import yaml

from test import ALL_MODES, DEBUG_MODES, TEST_RUNNER, TOP_SRC_DIR, TESTPY_PREPARED_ENVIRONMENT
from test.pylib.resource_gather import setup_worker_cgroup, get_resource_gather, run_resource_watcher
from test.pylib.util import get_xdist_worker_id
from test.pylib.suite.base import (
    SUITE_CONFIG_FILENAME,
    TestSuite,
    get_testpy_test,
    prepare_environment,
    init_testsuite_globals,
)
from test.pylib.util import get_modes_to_run

if TYPE_CHECKING:
    from collections.abc import Generator

    import _pytest.nodes
    import _pytest.scope

    from test.pylib.suite.base import Test


TEST_CONFIG_FILENAME = "test_config.yaml"

REPEATING_FILES = pytest.StashKey[set[pathlib.Path]]()
BUILD_MODE = pytest.StashKey[str]()
RUN_ID = pytest.StashKey[int]()
EXIT_MAXFAIL_REACHED = 11

logger = logging.getLogger(__name__)

# Store pytest config globally so we can access it in hooks that only receive report
_pytest_config: pytest.Config | None = None

_thread_pool_executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)
_stop_event = Event()
_future: Future | None = None

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
    parser.addoption('--test-py-init', action='store_true', default=False,
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
                     help="Set the pool_size for PythonTest and its descendants.  Alternatively environment variable "
                          "CLUSTER_POOL_SIZE can be used to achieve the same")
    parser.addoption("--extra-scylla-cmdline-options", default='',
                     help="Passing extra scylla cmdline options for all tests.  Options should be space separated:"
                          " '--logger-log-level raft=trace --default-log-level error'")
    parser.addoption('--x-log2-compaction-groups', action="store", default="0", type=int,
                     help="Controls number of compaction groups to be used by Scylla tests. Value of 3 implies 8 groups.")
    parser.addoption('--repeat', action="store", default="1", type=int,
                     help="number of times to repeat test execution")

    # Pass information about Scylla node from test.py to pytest.
    parser.addoption("--scylla-log-filename",
                     help="Path to a log file of a ScyllaDB node (for suites with type: Python)")


@pytest.fixture(autouse=True)
def print_scylla_log_filename(request: pytest.FixtureRequest) -> Generator[None]:
    """Print out a path to a ScyllaDB log.

    This is a fixture for Python test suites, because they are using a single node clusters created inside test.py,
    but it is handy to have this information printed to a pytest log.
    """

    yield

    if scylla_log_filename := request.config.getoption("--scylla-log-filename"):
        logger.info("ScyllaDB log file: %s", scylla_log_filename)


def testpy_test_fixture_scope(fixture_name: str, config: pytest.Config) -> _pytest.scope._ScopeName:
    """Dynamic scope for fixtures which rely on a current test.py suite/test.

    test.py runs tests file-by-file as separate pytest sessions, so, `session` scope is effectively close to be the
    same as `module` (can be a difference in the order.)  In case of running tests with bare pytest command, we
    need to use `module` scope to maintain same behavior as test.py, since we run all tests in one pytest session.
    """
    if getattr(config.option, "test_py_init", False):
        return "module"
    return "session"

testpy_test_fixture_scope.__test__ = False


# This is a constant used in `pytest_runtest_makereport` below to store the full report for the test case
# in a stash which can then be accessed from fixtures to print the stacktrace for the failed test
PHASE_REPORT_KEY = pytest.StashKey[dict[str, pytest.CollectReport]]()


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


@pytest.fixture(scope='function', autouse=True)
def run_gather_metrics(request: pytest.FixtureRequest, build_mode: str):
    temp_dir = pathlib.Path(request.config.getoption("--tmpdir")).absolute()

    # Get run_id from stash (set during collection) - it's the proper run ID
    params_stash = get_params_stash(node=request.node)
    run_id = params_stash[RUN_ID] if params_stash else request.config.getoption("--run_id")

    # Get the original test name (without .mode.run_id suffix)
    # Test name format is: "original_name.mode.run_id"
    test_name = request.node.name
    suffix = f".{build_mode}.{run_id}"
    if test_name.endswith(suffix):
        original_test_name = test_name[:-len(suffix)]
    else:
        original_test_name = test_name

    # Get file path and derive suite info
    file_path = request.node.path
    suite_path = file_path.parent  # Directory containing the test file
    file_name = file_path.name  # The actual file name with extension

    test = SimpleNamespace(
        time_end=0,
        time_start=time.time(),
        id=run_id,
        mode=build_mode,
        success=False,
        path=file_path,
        shortname=original_test_name,
        suite=SimpleNamespace(
            log_dir=temp_dir / build_mode,  # log_dir should be testlog/<mode>
            name=suite_path.name,  # Suite name is the directory name
            suite_path=suite_path,  # Suite path is the directory containing the test
            test_file_name=file_name,  # Actual file name with extension
        ),
    )
    resource_gather = get_resource_gather(
        temp_dir=pathlib.Path(request.config.getoption("--tmpdir")),
        is_switched_on=request.config.getoption("--gather-metrics"),
        test=test,
        worker_id=get_xdist_worker_id(),
    )

    # Create test cgroup and move processes to it
    resource_gather.make_cgroup()
    resource_gather.cgroup_monitor()

    yield
    test.time_end = time.time()
    resource_gather.stop_monitoring()

    # Get test result and timing
    # test.time_end = time.time()
    report = request.node.stash[PHASE_REPORT_KEY]
    success = report.when == "call" and not report.failed

    # Write metrics to database
    resource_gather.write_metrics_to_db(
        metrics=resource_gather.get_test_metrics(),
        success=success
    )

    # Move processes back to worker cgroup and remove test cgroup
    resource_gather.remove_cgroup()

@pytest.fixture(scope=testpy_test_fixture_scope, autouse=True)
def build_mode(request: pytest.FixtureRequest) -> str:
    params_stash = get_params_stash(node=request.node)
    if params_stash is None:
        return request.config.build_modes[0]
    return params_stash[BUILD_MODE]


@pytest.fixture(scope=testpy_test_fixture_scope)
async def testpy_test(request: pytest.FixtureRequest, build_mode: str) -> Test | None:
    """Create an instance of Test class for the current test.py test."""

    if request.scope == "module":
        return await get_testpy_test(path=request.path, options=request.config.option, mode=build_mode)
    return None


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        modify_pytest_item(item=item)

    suites_order = defaultdict(count().__next__)  # number suites in order of appearance

    def sort_key(item: pytest.Item) -> tuple[int, bool]:
        suite = item.stash[TEST_SUITE]
        return suites_order[suite], suite and item.path.stem not in suite.cfg.get("run_first", [])

    items.sort(key=sort_key)


def pytest_sessionstart(session: pytest.Session) -> None:
    # test.py starts S3 mock and create/cleanup testlog by itself. Also, if we run with --collect-only option,
    # we don't need this stuff.
    global _future, _stop_event, _thread_pool_executor
    gather_metrics = session.config.getoption("--gather-metrics")
    temp_dir = pathlib.Path(session.config.getoption("--tmpdir")).absolute()
    save_log_on_success = session.config.getoption("--save-log-on-success")
    toxiproxy_byte_limit = session.config.getoption("--byte-limit")
    collect_only = session.config.getoption("--collect-only")
    testpy_init = session.config.getoption("--test-py-init")
    if TEST_RUNNER != "pytest" or collect_only:
        return

    if not testpy_init:
        return

    # Check if this is an xdist worker
    is_xdist_worker = xdist.is_xdist_worker(request_or_session=session)

    # Always initialize globals in xdist workers (they run in separate processes)
    # For the main process, only init if test.py hasn't done so already
    if is_xdist_worker or TESTPY_PREPARED_ENVIRONMENT not in os.environ:
        init_testsuite_globals()
        TestSuite.artifacts.add_exit_artifact(None, TestSuite.hosts.cleanup)

    # Run stuff just once for the main pytest process (not in xdist workers).
    # Only prepare the environment if it hasn't been prepared by test.py
    if not is_xdist_worker and TESTPY_PREPARED_ENVIRONMENT not in os.environ:

        prepare_environment(
            tempdir_base=temp_dir,
            modes=get_modes_to_run(session.config),
            gather_metrics=gather_metrics,
            save_log_on_success=save_log_on_success,
            toxiproxy_byte_limit=toxiproxy_byte_limit,
        )
    if gather_metrics:
        setup_worker_cgroup()
        _future = _thread_pool_executor.submit( run_resource_watcher, gather_metrics, _stop_event,temp_dir)



@pytest.hookimpl(trylast=True)
def pytest_runtest_logreport(report):
    """Add custom XML attributes to JUnit testcase elements.

    This hook wraps the node_reporter's to_xml method to add custom attributes
    when the XML element is created. This approach works with pytest-xdist because
    it modifies the XML element directly when it's generated, rather than trying
    to modify attrs before finalize() is called.

    Attributes added:
    - function_path: The function path of the test case (excluding parameters).

    Uses trylast=True to run after LogXML's hook has created the node_reporter.
    """
    from _pytest.junitxml import xml_key

    # Only process call phase
    if report.when != "call":
        return

    # Get the XML reporter
    config = _pytest_config
    if config is None:
        return

    xml = config.stash.get(xml_key, None)
    if xml is None:
        return

    node_reporter = xml.node_reporter(report)

    nodeid = report.nodeid
    function_path = f'test/{nodeid.rsplit('.', 2)[0].rsplit('[', 1)[0]}'

    # Wrap the to_xml method to add custom attributes to the element
    original_to_xml = node_reporter.to_xml

    def custom_to_xml():
        """Wrapper that adds custom attributes to the testcase element."""
        element = original_to_xml()
        element.set("function_path", function_path)
        return element

    node_reporter.to_xml = custom_to_xml


def pytest_sessionfinish(session: pytest.Session) -> None:
    if not session.config.getoption("--test-py-init"):
        return

    if _future:
        _stop_event.set()
        _future.result()

    # Check if this is an xdist worker - workers should not clean up (only the main process should)
    # Check if test.py has already prepared the environment, so it should clean up
    is_xdist_worker = xdist.is_xdist_worker(request_or_session=session)
    if is_xdist_worker or TESTPY_PREPARED_ENVIRONMENT in os.environ:
        return
    # we only clean up when running with pure pytest
    if getattr(TestSuite, "artifacts", None) is not None:
        asyncio.run(TestSuite.artifacts.cleanup_before_exit())

    # Modify exit code to reflect the number of failed tests for easier detection in CI.
    maxfail = session.config.getoption("maxfail")

    if 0 < maxfail <= session.testsfailed:
        session.exitstatus = EXIT_MAXFAIL_REACHED

def pytest_configure(config: pytest.Config) -> None:
    global _pytest_config
    _pytest_config = config

    os.environ["TOPOLOGY_RANDOM_FAILURES_TEST_SHUFFLE_SEED"] = os.environ.get("TOPOLOGY_RANDOM_FAILURES_TEST_SHUFFLE_SEED", str(random.randint(0, sys.maxsize)))
    config.build_modes = get_modes_to_run(config)
    repeat = int(config.getoption("--repeat"))

    if testpy_run_id := config.getoption("--run_id"):
        if repeat != 1:
            raise RuntimeError("Can't use --run_id and --repeat simultaneously.")
        config.run_ids = (testpy_run_id,)
    else:
        config.run_ids = tuple(range(1, repeat + 1))


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
        repeats = list(product(build_modes, parent.config.run_ids))

        if not repeats:
            return []

        ihook = parent.ihook
        collectors = list(chain(collectors, chain.from_iterable(
            ihook.pytest_collect_file(file_path=file_path, parent=parent) for _ in range(1, len(repeats))
        )))
        for (build_mode, run_id), collector in zip(repeats, collectors, strict=True):
            collector.stash[BUILD_MODE] = build_mode
            collector.stash[RUN_ID] = run_id
            collector.stash[TEST_SUITE] = suite_config

        parent.stash[REPEATING_FILES].remove(file_path)

    return collectors


class TestSuiteConfig:
    def __init__(self, config_file: pathlib.Path):
        self.path = config_file.parent
        self.cfg = yaml.safe_load(config_file.read_text(encoding="utf-8"))

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
        for config_file in (node.path / SUITE_CONFIG_FILENAME, node.path / TEST_CONFIG_FILENAME,):
            if config_file.is_file():
                suite = cls(config_file=config_file)
                break
        else:
            if node.parent is None:
                return None
            suite = node.parent.stash.get(TEST_SUITE, None)
            if suite is None:
                suite = cls.from_pytest_node(node=node.parent)
        if suite:
            node.stash[TEST_SUITE] = suite
        return suite


TEST_SUITE = pytest.StashKey[TestSuiteConfig | None]()

_STASH_KEYS_TO_COPY = BUILD_MODE, RUN_ID, TEST_SUITE


def get_params_stash(node: _pytest.nodes.Node) -> pytest.Stash | None:
    parent = node.getparent(cls=pytest.File)
    if parent is None:
        return None
    return parent.stash


def modify_pytest_item(item: pytest.Item) -> None:
    params_stash = get_params_stash(node=item)

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
            if mode == item.stash[BUILD_MODE]:
                if platform_key is None or platform_key in platform.platform():
                    item.add_marker(pytest.mark.skip(reason=reason))
        try:
            __skip_test(*mark.args, **mark.kwargs)
        except TypeError as e:
            raise TypeError(f"Failed to process skip_mode mark, {mark} for test {item}, error {e}")
