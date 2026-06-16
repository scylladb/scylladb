#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import collections
import re
import logging
import os
import pathlib
import shutil
import sys
import time
from abc import ABC, abstractmethod
from importlib import import_module
from typing import TYPE_CHECKING

import colorama
import universalasync
import yaml

from test import TEST_DIR, TEST_RUNNER
from test.pylib.artifact_registry import ArtifactRegistry
from test.pylib.host_registry import HostRegistry
from test.pylib.ldap_server import start_ldap
from test.pylib.minio_server import MinioServer
from test.pylib.resource_gather import setup_cgroup
from test.pylib.s3_proxy import S3ProxyServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.util import LogPrefixAdapter, get_xdist_worker_id
from test.pylib.version_fetch_utils import fetch_and_install_scylla_version
if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any, List


TEST_CONFIG_FILENAME = "test_config.yaml"
PYTEST_TESTS_LOGS_FOLDER = "pytest_tests_logs"

output_is_a_tty = sys.stdout.isatty()


def create_formatter(*decorators) -> Callable[[Any], str]:
    """Return a function which decorates its argument with the given
    color/style if stdout is a tty, and leaves intact otherwise."""
    def color(arg: Any) -> str:
        return "".join(decorators) + str(arg) + colorama.Style.RESET_ALL

    def nocolor(arg: Any) -> str:
        return str(arg)
    return color if output_is_a_tty else nocolor


class palette:
    """Color palette for formatting terminal output"""
    ok = create_formatter(colorama.Fore.GREEN, colorama.Style.BRIGHT)
    fail = create_formatter(colorama.Fore.RED, colorama.Style.BRIGHT)
    new = create_formatter(colorama.Fore.BLUE)
    skip = create_formatter(colorama.Style.DIM)
    path = create_formatter(colorama.Style.BRIGHT)
    diff_in = create_formatter(colorama.Fore.GREEN)
    diff_out = create_formatter(colorama.Fore.RED)
    diff_mark = create_formatter(colorama.Fore.MAGENTA)
    warn = create_formatter(colorama.Fore.YELLOW)
    crit = create_formatter(colorama.Fore.RED, colorama.Style.BRIGHT)
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    @staticmethod
    def nocolor(text: str) -> str:
        return palette.ansi_escape.sub('', text)


class TestSuite(ABC):
    """A test suite is a folder with tests of the same type.
    E.g. it can be unit tests, boost tests, or CQL tests."""

    # All existing test suites, one suite per path/mode.

    suites: dict[str, TestSuite] = {}

    artifacts: ArtifactRegistry

    _next_id = collections.defaultdict(int) # (test_key -> id)

    def __init__(self, path: str, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        self.suite_path = pathlib.Path(path)
        self.log_dir = pathlib.Path(options.tmpdir) / mode
        self.name = str(self.suite_path.name)
        self.cfg = cfg
        self.options = options
        self.mode = mode
        self.suite_key = os.path.join(path, mode)
        self.tests: List['Test'] = []
        # environment variables that should be the base of all processes running in this suit
        self.base_env = {}
        if self.need_coverage():
            # Set the coverage data from each instrumented object to use the same file (and merged into it with locking)
            # as long as we don't need test specific coverage data, this looks sufficient. The benefit of doing this in
            # this way is that the storage will not be bloated with coverage files (each can weigh 10s of MBs so for several
            # thousands of tests it can easily reach 10 of GBs)
            # ref: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html#running-the-instrumented-program
            self.base_env["LLVM_PROFILE_FILE"] = str(self.log_dir / "coverage" / self.name / "%m.profraw")


    # Generate a unique ID for `--repeat`ed tests
    # We want these tests to have different XML IDs so test result
    # processors (Jenkins) don't merge results for different iterations of
    # the same test. We also don't want the ids to be too random, because then
    # there is no correlation between test identifiers across multiple
    # runs of test.py, and so it's hard to understand failure trends. The
    # compromise is for next_id() results to be unique only within a particular
    # test case. That is, we'll have a.1, a.2, a.3, b.1, b.2, b.3 rather than
    # a.1 a.2 a.3 b.4 b.5 b.6.
    def next_id(self, test_key) -> int:
        if getattr(self.options, "run_id", None):
            TestSuite._next_id[test_key] = self.options.run_id
        else:
            TestSuite._next_id[test_key] += 1
        return TestSuite._next_id[test_key]


    @staticmethod
    def load_cfg(path: pathlib.Path) -> dict:
        with path.open(encoding='utf-8') as cfg_file:
            cfg = yaml.safe_load(cfg_file.read())
            if not isinstance(cfg, dict):
                raise RuntimeError(f"Failed to load tests config file {path}")
            return cfg

    @staticmethod
    def opt_create(config: pathlib.Path, options: argparse.Namespace, mode: str) -> 'TestSuite':
        """Return a subclass of TestSuite with name cfg["type"].title + TestSuite.
        Ensures there is only one suite instance per path."""
        path = str(config.parent)
        suite_key = os.path.join(path, mode)
        suite = TestSuite.suites.get(suite_key)
        if not suite:
            cfg = TestSuite.load_cfg(config)
            kind = cfg.get("type")
            if kind is None:
                raise RuntimeError("Failed to load tests in {}: test_config.yaml has no suite type".format(path))

            def suite_type_to_class_name(suite_type: str) -> str:
                return suite_type.title() + "TestSuite"

            SpecificTestSuite = getattr(import_module("test.pylib.suite"), suite_type_to_class_name(kind), None)
            if not SpecificTestSuite:
                raise RuntimeError("Failed to load tests in {}: suite type '{}' not found".format(path, kind))
            suite = SpecificTestSuite(path, cfg, options, mode)
            assert suite is not None
            TestSuite.suites[suite_key] = suite
        return suite


    @abstractmethod
    async def add_test(self, shortname: str, casename: str | None) -> None:
        pass

    def need_coverage(self):
        return self.options.coverage and (self.mode in self.options.coverage_modes) and bool(self.cfg.get("coverage",True))


class Test:
    """Base class for CQL, Unit and Boost tests"""
    def __init__(self, test_no: int, shortname: str, suite) -> None:
        self.id = test_no
        # Name with test suite name
        self.name = os.path.join(suite.name, shortname.split('.')[0])
        # Name within the suite
        self.shortname = shortname
        self.mode = suite.mode
        self.suite = suite
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = f"{self.suite.name}.{self.shortname.replace('/', '_')}.{self.id}"
        if xdist_worker_id := get_xdist_worker_id():
            self.uname = f"{xdist_worker_id}.{self.uname}"
        self.log_filename = self.suite.log_dir / f"{self.uname}.log"
        self.success = False
        self.time_start: float = 0
        self.time_end: float = 0


def init_testsuite_globals() -> None:
    """Create global objects required for a test run."""

    TestSuite.artifacts = ArtifactRegistry()


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

@universalasync.async_to_sync_wraps
async def prepare_environment(tempdir_base: pathlib.Path, modes: list[str], gather_metrics: bool, save_log_on_success: bool,
                        toxiproxy_byte_limit: int) -> None:
    prepare_dirs(tempdir_base, modes, gather_metrics, save_log_on_success=save_log_on_success)
    await start_3rd_party_services(tempdir_base=tempdir_base, toxiproxy_byte_limit=toxiproxy_byte_limit)

def prepare_dirs(tempdir_base: pathlib.Path, modes: list[str], gather_metrics: bool, save_log_on_success: bool = False) -> None:
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
        if TEST_RUNNER != "pytest":
            prepare_dir(tempdir_base / mode / "pytest", "*", save_log_on_success)


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

    TestSuite.artifacts.add_exit_artifact(None, make_async_finalize)
    ms = MinioServer(
        tempdir_base=str(tempdir_base),
        address=await hosts.lease_host(),
        logger=LogPrefixAdapter(
            logger=_make_service_logger("minio", tempdir_base / "minio.log"),
            extra={"prefix": "minio"},
        ),
    )
    await ms.start()
    TestSuite.artifacts.add_exit_artifact(None, ms.stop)

    mock_s3_server = MockS3Server(
        host=await hosts.lease_host(),
        port=2012,
        logger=LogPrefixAdapter(
            logger=_make_service_logger("s3_mock", tempdir_base / "s3_mock.log"),
            extra={"prefix": "s3_mock"},
        ),
    )
    await mock_s3_server.start()
    TestSuite.artifacts.add_exit_artifact(None, mock_s3_server.stop)

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
    TestSuite.artifacts.add_exit_artifact(None, proxy_s3_server.stop)

def find_suite_config(path: pathlib.Path, config_filename: str) -> pathlib.Path:
    for directory in (path.joinpath("_") if path.is_dir() else path).absolute().relative_to(TEST_DIR).parents:
        suite_config = TEST_DIR / directory / config_filename
        if suite_config.exists():
            return suite_config
    raise FileNotFoundError(f"Unable to find a suite config file ({config_filename}) related to {path}")


async def get_testpy_test(path: pathlib.Path, options: argparse.Namespace, mode: str) -> Test:
    """Create an instance of Test class for the path provided."""
    suite_config = find_suite_config(path=path, config_filename=TEST_CONFIG_FILENAME)
    suite = TestSuite.opt_create(config=suite_config, options=options, mode=mode)
    if getattr(options, "exe_path", False):
        suite.scylla_exe = options.exe_path
    elif getattr(options, "exe_url", False):
        suite.scylla_exe = fetch_and_install_scylla_version(url=options.exe_url)
    await suite.add_test(shortname=str(path.relative_to(suite.suite_path).with_suffix("")), casename=None)
    return suite.tests[-1]
