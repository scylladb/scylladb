#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-present ScyllaDB
#
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import argparse
import asyncio
import collections
import colorama
import difflib
import filecmp
import glob
import itertools
import logging
import multiprocessing
import os
import pathlib
import re
import resource
import shlex
import shutil
import signal
import subprocess
import sys
import time
import traceback
import xml.etree.ElementTree as ET
import yaml

from abc import ABC, abstractmethod
from io import StringIO
from scripts import coverage    # type: ignore
from test.pylib.artifact_registry import ArtifactRegistry
from test.pylib.host_registry import HostRegistry
from test.pylib.pool import Pool
from test.pylib.util import LogPrefixAdapter
from test.pylib.scylla_cluster import ScyllaServer, ScyllaCluster, get_cluster_manager, merge_cmdline_options
from test.pylib.minio_server import MinioServer
from typing import Dict, List, Callable, Any, Iterable, Optional, Awaitable, Union
import logging
from test.pylib import coverage_utils
import humanfriendly
import treelib

launch_time = time.monotonic()

output_is_a_tty = sys.stdout.isatty()

all_modes = {'debug': 'Debug',
             'release': 'RelWithDebInfo',
             'dev': 'Dev',
             'sanitize': 'Sanitize',
             'coverage': 'Coverage'}
debug_modes = set(['debug', 'sanitize'])

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


def path_to(mode, *components):
    """Resolve path to built executable"""
    build_dir = 'build'
    if os.path.exists(os.path.join(build_dir, 'build.ninja')):
        *dir_components, basename = components
        exe_path = os.path.join(build_dir, *dir_components, all_modes[mode], basename)
    else:
        exe_path = os.path.join(build_dir, mode, *components)
    if not os.access(exe_path, os.F_OK):
        raise FileNotFoundError(f"{exe_path} does not exist.")
    elif not os.access(exe_path, os.X_OK):
        raise PermissionError(f"{exe_path} is not executable.")
    return exe_path


def ninja(target):
    """Build specified target using ninja"""
    build_dir = 'build'
    args = ['ninja', target]
    if os.path.exists(os.path.join(build_dir, 'build.ninja')):
        args = ['ninja', '-C', build_dir, target]
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0].decode()


class TestSuite(ABC):
    """A test suite is a folder with tests of the same type.
    E.g. it can be unit tests, boost tests, or CQL tests."""

    # All existing test suites, one suite per path/mode.
    suites: Dict[str, 'TestSuite'] = dict()
    artifacts = ArtifactRegistry()
    hosts = HostRegistry()
    FLAKY_RETRIES = 5
    _next_id = collections.defaultdict(int) # (test_key -> id)

    def __init__(self, path: str, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        self.suite_path = pathlib.Path(path)
        self.name = str(self.suite_path.name)
        self.cfg = cfg
        self.options = options
        self.mode = mode
        self.suite_key = os.path.join(path, mode)
        self.tests: List['Test'] = []
        self.pending_test_count = 0
        # The number of failed tests
        self.n_failed = 0

        self.run_first_tests = set(cfg.get("run_first", []))
        self.no_parallel_cases = set(cfg.get("no_parallel_cases", []))
        # Skip tests disabled in suite.yaml
        self.disabled_tests = set(self.cfg.get("disable", []))
        # Skip tests disabled in specific mode.
        self.disabled_tests.update(self.cfg.get("skip_in_" + mode, []))
        self.flaky_tests = set(self.cfg.get("flaky", []))
        # If this mode is one of the debug modes, and there are
        # tests disabled in a debug mode, add these tests to the skip list.
        if mode in debug_modes:
            self.disabled_tests.update(self.cfg.get("skip_in_debug_modes", []))
        # If a test is listed in run_in_<mode>, it should only be enabled in
        # this mode. Tests not listed in any run_in_<mode> directive should
        # run in all modes. Inversing this, we should disable all tests
        # which are listed explicitly in some run_in_<m> where m != mode
        # This of course may create ambiguity with skip_* settings,
        # since the priority of the two is undefined, but oh well.
        run_in_m = set(self.cfg.get("run_in_" + mode, []))
        for a in all_modes:
            if a == mode:
                continue
            skip_in_m = set(self.cfg.get("run_in_" + a, []))
            self.disabled_tests.update(skip_in_m - run_in_m)
        # environment variables that should be the base of all processes running in this suit
        self.base_env = {}
        if self.need_coverage():
            # Set the coverage data from each instrumented object to use the same file (and merged into it with locking)
            # as long as we don't need test specific coverage data, this looks sufficient. The benefit of doing this in
            # this way is that the storage will not be bloated with coverage files (each can weigh 10s of MBs so for several
            # thousands of tests it can easily reach 10 of GBs)
            # ref: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html#running-the-instrumented-program
            self.base_env["LLVM_PROFILE_FILE"] = os.path.join(options.tmpdir,self.mode, "coverage", self.name, "%m.profraw")
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
        TestSuite._next_id[test_key] += 1
        return TestSuite._next_id[test_key]

    @staticmethod
    def test_count() -> int:
        return sum(TestSuite._next_id.values())

    @staticmethod
    def load_cfg(path: str) -> dict:
        with open(os.path.join(path, "suite.yaml"), "r") as cfg_file:
            cfg = yaml.safe_load(cfg_file.read())
            if not isinstance(cfg, dict):
                raise RuntimeError("Failed to load tests in {}: suite.yaml is empty".format(path))
            return cfg

    @staticmethod
    def opt_create(path: str, options: argparse.Namespace, mode: str) -> 'TestSuite':
        """Return a subclass of TestSuite with name cfg["type"].title + TestSuite.
        Ensures there is only one suite instance per path."""
        suite_key = os.path.join(path, mode)
        suite = TestSuite.suites.get(suite_key)
        if not suite:
            cfg = TestSuite.load_cfg(path)
            kind = cfg.get("type")
            if kind is None:
                raise RuntimeError("Failed to load tests in {}: suite.yaml has no suite type".format(path))

            def suite_type_to_class_name(suite_type: str) -> str:
                if suite_type.casefold() == "Approval".casefold():
                    suite_type = "CQLApproval"
                else:
                    suite_type = suite_type.title()
                return suite_type + "TestSuite"

            SpecificTestSuite = globals().get(suite_type_to_class_name(kind))
            if not SpecificTestSuite:
                raise RuntimeError("Failed to load tests in {}: suite type '{}' not found".format(path, kind))
            suite = SpecificTestSuite(path, cfg, options, mode)
            assert suite is not None
            TestSuite.suites[suite_key] = suite
        return suite

    @staticmethod
    def all_tests() -> Iterable['Test']:
        return itertools.chain(*[suite.tests for suite in
                                 TestSuite.suites.values()])

    @property
    @abstractmethod
    def pattern(self) -> str:
        pass

    @abstractmethod
    async def add_test(self, shortname: str, casename: str) -> None:
        pass

    async def run(self, test: 'Test', options: argparse.Namespace):
        try:
            for i in range(1, self.FLAKY_RETRIES):
                if i > 1:
                    test.is_flaky_failure = True
                    logging.info("Retrying test %s after a flaky fail, retry %d", test.uname, i)
                    test.reset()
                await test.run(options)
                if test.success or not test.is_flaky or test.is_cancelled:
                    break
        finally:
            self.pending_test_count -= 1
            self.n_failed += int(not test.success)
            if self.pending_test_count == 0:
                await TestSuite.artifacts.cleanup_after_suite(self, self.n_failed > 0)
        return test

    def junit_tests(self):
        """Tests which participate in a consolidated junit report"""
        return self.tests

    def boost_tests(self):
        return []

    def build_test_list(self) -> List[str]:
        return [os.path.splitext(t.relative_to(self.suite_path))[0] for t in
                self.suite_path.glob(self.pattern)]

    async def add_test_list(self) -> None:
        options = self.options
        lst = self.build_test_list()
        if lst:
            # Some tests are long and are better to be started earlier,
            # so pop them up while sorting the list
            lst.sort(key=lambda x: (x not in self.run_first_tests, x))

        pending = set()
        for shortname in lst:
            testname = os.path.join(self.name, shortname)
            casename = None

            # Check opt-out lists
            if shortname in self.disabled_tests:
                continue
            if options.skip_patterns:
                if any(skip_pattern in testname for skip_pattern in options.skip_patterns):
                    continue

            # Check opt-in list
            if options.name:
                for p in options.name:
                    pn = p.split('::', 2)
                    if len(pn) == 1 and p in testname:
                        break
                    if len(pn) == 2 and pn[0] == testname:
                        if pn[1] != "*":
                            casename = pn[1]
                        break
                else:
                    continue

            async def add_test(shortname, casename) -> None:
                # Add variants of the same test sequentially
                # so that case cache has a chance to populate
                for i in range(options.repeat):
                    await self.add_test(shortname, casename)
                    self.pending_test_count += 1

            pending.add(asyncio.create_task(add_test(shortname, casename)))

        if len(pending) == 0:
            return
        try:
            await asyncio.gather(*pending)
        except asyncio.CancelledError:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise
    def need_coverage(self):
        return self.options.coverage and (self.mode in self.options.coverage_modes) and bool(self.cfg.get("coverage",True))

class UnitTestSuite(TestSuite):
    """TestSuite instantiation for non-boost unit tests"""

    def __init__(self, path: str, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        # Map of custom test command line arguments, if configured
        self.custom_args = cfg.get("custom_args", {})
        # Map of tests that cannot run with compaction groups
        self.all_can_run_compaction_groups_except = cfg.get("all_can_run_compaction_groups_except")

    async def create_test(self, shortname, casename, suite, args):
        exe = path_to(suite.mode, "test", suite.name, shortname)
        if not os.access(exe, os.X_OK):
            print(palette.warn(f"Unit test executable {exe} not found."))
            return
        test = UnitTest(self.next_id((shortname, self.suite_key)), shortname, suite, args)
        self.tests.append(test)

    async def add_test(self, shortname, casename) -> None:
        """Create a UnitTest class with possibly custom command line
        arguments and add it to the list of tests"""
        # Skip tests which are not configured, and hence are not built
        if os.path.join("test", self.name, shortname) not in self.options.tests:
            return

        # Default seastar arguments, if not provided in custom test options,
        # are two cores and 2G of RAM
        args = self.custom_args.get(shortname, ["-c2 -m2G"])
        args = merge_cmdline_options(args, self.options.extra_scylla_cmdline_options)
        for a in args:
            await self.create_test(shortname, casename, self, a)

    @property
    def pattern(self) -> str:
        return "*_test.cc"


class BoostTestSuite(UnitTestSuite):
    """TestSuite for boost unit tests"""

    # A cache of individual test cases, for which we have called
    # --list_content. Static to share across all modes.
    _case_cache: Dict[str, List[str]] = dict()

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode) -> None:
        super().__init__(path, cfg, options, mode)

    async def create_test(self, shortname: str, casename: str, suite, args) -> None:
        exe = path_to(suite.mode, "test", suite.name, shortname)
        if not os.access(exe, os.X_OK):
            print(palette.warn(f"Boost test executable {exe} not found."))
            return
        options = self.options
        allows_compaction_groups = self.all_can_run_compaction_groups_except != None and shortname not in self.all_can_run_compaction_groups_except
        if options.parallel_cases and (shortname not in self.no_parallel_cases) and casename is None:
            fqname = os.path.join(self.mode, self.name, shortname)
            if fqname not in self._case_cache:
                process = await asyncio.create_subprocess_exec(
                    exe, *['--list_content'],
                    stderr=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    env=dict(os.environ,
                             **{"ASAN_OPTIONS": "halt_on_error=0"}),
                    preexec_fn=os.setsid,
                )
                _, stderr = await asyncio.wait_for(process.communicate(), options.timeout)

                case_list = [case[:-1] for case in stderr.decode().splitlines() if case.endswith('*')]
                self._case_cache[fqname] = case_list

            case_list = self._case_cache[fqname]
            if len(case_list) == 1:
                test = BoostTest(self.next_id((shortname, self.suite_key)), shortname, suite, args, None, allows_compaction_groups)
                self.tests.append(test)
            else:
                for case in case_list:
                    test = BoostTest(self.next_id((shortname, self.suite_key, case)), shortname, suite, args, case, allows_compaction_groups)
                    self.tests.append(test)
        else:
            test = BoostTest(self.next_id((shortname, self.suite_key)), shortname, suite, args, casename, allows_compaction_groups)
            self.tests.append(test)

    def junit_tests(self) -> Iterable['Test']:
        """Boost tests produce an own XML output, so are not included in a junit report"""
        return []

    def boost_tests(self) -> Iterable['Tests']:
        return self.tests

class PythonTestSuite(TestSuite):
    """A collection of Python pytests against a single Scylla instance"""

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = path_to(self.mode, "scylla")
        self.scylla_env = dict(self.base_env)
        if self.mode == "coverage":
            self.scylla_env.update(coverage.env(self.scylla_exe, distinct_id=self.name))
        self.scylla_env['SCYLLA'] = self.scylla_exe

        cluster_cfg = self.cfg.get("cluster", {"initial_size": 1})
        cluster_size = cluster_cfg["initial_size"]
        env_pool_size = os.getenv("CLUSTER_POOL_SIZE")
        if options.cluster_pool_size is not None:
            pool_size = options.cluster_pool_size
        elif env_pool_size is not None:
            pool_size = int(env_pool_size)
        else:
            pool_size = cfg.get("pool_size", 2)
        self.dirties_cluster = set(cfg.get("dirties_cluster", []))

        self.create_cluster = self.get_cluster_factory(cluster_size, options)
        async def recycle_cluster(cluster: ScyllaCluster) -> None:
            """When a dirty cluster is returned to the cluster pool,
               stop it and release the used IPs. We don't necessarily uninstall() it yet,
               which would delete the log file and directory - we might want to preserve
               these if it came from a failed test.
            """
            for srv in cluster.running.values():
                srv.log_file.close()
                srv.maintenance_socket_dir.cleanup()
            await cluster.stop()
            await cluster.release_ips()

        self.clusters = Pool(pool_size, self.create_cluster, recycle_cluster)

    def get_cluster_factory(self, cluster_size: int, options: argparse.Namespace) -> Callable[..., Awaitable]:
        def create_server(create_cfg: ScyllaCluster.CreateServerParams):
            cmdline_options = self.cfg.get("extra_scylla_cmdline_options", [])
            if type(cmdline_options) == str:
                cmdline_options = [cmdline_options]
            cmdline_options = merge_cmdline_options(cmdline_options, create_cfg.cmdline_from_test)
            cmdline_options = merge_cmdline_options(cmdline_options, options.extra_scylla_cmdline_options)
            # There are multiple sources of config options, with increasing priority
            # (if two sources provide the same config option, the higher priority one wins):
            # 1. the defaults
            # 2. suite-specific config options (in "extra_scylla_config_options")
            # 3. config options from tests (when servers are added during a test)
            default_config_options = \
                    {"authenticator": "PasswordAuthenticator",
                     "authorizer": "CassandraAuthorizer"}
            config_options = default_config_options | \
                             self.cfg.get("extra_scylla_config_options", {}) | \
                             create_cfg.config_from_test

            server = ScyllaServer(
                mode=self.mode,
                exe=self.scylla_exe,
                vardir=os.path.join(self.options.tmpdir, self.mode),
                logger=create_cfg.logger,
                cluster_name=create_cfg.cluster_name,
                ip_addr=create_cfg.ip_addr,
                seeds=create_cfg.seeds,
                cmdline_options=cmdline_options,
                config_options=config_options,
                property_file=create_cfg.property_file,
                append_env=self.base_env,
                server_encryption=create_cfg.server_encryption)

            return server

        async def create_cluster(logger: Union[logging.Logger, logging.LoggerAdapter]) -> ScyllaCluster:
            cluster = ScyllaCluster(logger, self.hosts, cluster_size, create_server)

            async def stop() -> None:
                await cluster.stop()

            # Suite artifacts are removed when
            # the entire suite ends successfully.
            self.artifacts.add_suite_artifact(self, stop)
            if not self.options.save_log_on_success:
                # If a test fails, we might want to keep the data dirs.
                async def uninstall() -> None:
                    await cluster.uninstall()

                self.artifacts.add_suite_artifact(self, uninstall)
            self.artifacts.add_exit_artifact(self, stop)

            await cluster.install_and_start()
            return cluster

        return create_cluster

    def build_test_list(self) -> List[str]:
        """For pytest, search for directories recursively"""
        path = self.suite_path
        pytests = itertools.chain(path.rglob("*_test.py"), path.rglob("test_*.py"))
        return [os.path.splitext(t.relative_to(self.suite_path))[0] for t in pytests]

    @property
    def pattern(self) -> str:
        assert False

    async def add_test(self, shortname, casename) -> None:
        test = PythonTest(self.next_id((shortname, self.suite_key)), shortname, casename, self)
        self.tests.append(test)


class CQLApprovalTestSuite(PythonTestSuite):
    """Run CQL commands against a single Scylla instance"""

    def __init__(self, path, cfg, options: argparse.Namespace, mode) -> None:
        super().__init__(path, cfg, options, mode)

    def build_test_list(self) -> List[str]:
        return TestSuite.build_test_list(self)

    async def add_test(self, shortname: str, casename: str) -> None:
        test = CQLApprovalTest(self.next_id((shortname, self.suite_key)), shortname, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        return "*test.cql"


class TopologyTestSuite(PythonTestSuite):
    """A collection of Python pytests against Scylla instances dealing with topology changes.
       Instead of using a single Scylla cluster directly, there is a cluster manager handling
       the lifecycle of clusters and bringing up new ones as needed. The cluster health checks
       are done per test case.
    """

    def build_test_list(self) -> List[str]:
        """Build list of Topology python tests"""
        return TestSuite.build_test_list(self)

    async def add_test(self, shortname: str, casename: str) -> None:
        """Add test to suite"""
        test = TopologyTest(self.next_id((shortname, 'topology', self.mode)), shortname, casename, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        """Python pattern"""
        return "test_*.py"

    def junit_tests(self):
        """Return an empty list, since topology tests are excluded from an aggregated Junit report to prevent double
        count in the CI report"""
        return []


class RunTestSuite(TestSuite):
    """TestSuite for test directory with a 'run' script """

    def __init__(self, path: str, cfg, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = path_to(self.mode, "scylla")
        self.scylla_env = dict(self.base_env)
        if self.mode == "coverage":
            self.scylla_env = coverage.env(self.scylla_exe, distinct_id=self.name)

        self.scylla_env['SCYLLA'] = self.scylla_exe

    async def add_test(self, shortname, casename) -> None:
        test = RunTest(self.next_id((shortname, self.suite_key)), shortname, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        return "run"


class ToolTestSuite(TestSuite):
    """A collection of Python pytests that test tools

    These tests do not need an cluster setup for them. They invoke scylla
    manually, in tool mode.
    """

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)

    def build_test_list(self) -> List[str]:
        """For pytest, search for directories recursively"""
        path = self.suite_path
        pytests = itertools.chain(path.rglob("*_test.py"), path.rglob("test_*.py"))
        return [os.path.splitext(t.relative_to(self.suite_path))[0] for t in pytests]

    @property
    def pattern(self) -> str:
        assert False

    async def add_test(self, shortname, casename) -> None:
        test = ToolTest(self.next_id((shortname, self.suite_key)), shortname, self)
        self.tests.append(test)


class Test:
    """Base class for CQL, Unit and Boost tests"""
    def __init__(self, test_no: int, shortname: str, suite) -> None:
        self.id = test_no
        self.path = ""
        self.args: List[str] = []
        self.valid_exit_codes = [0]
        # Name with test suite name
        self.name = os.path.join(suite.name, shortname.split('.')[0])
        # Name within the suite
        self.shortname = shortname
        self.mode = suite.mode
        self.suite = suite
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = "{}.{}.{}".format(self.suite.name, self.shortname, self.id)
        self.log_filename = pathlib.Path(suite.options.tmpdir) / self.mode / (self.uname + ".log")
        self.log_filename.parent.mkdir(parents=True, exist_ok=True)
        self.is_flaky = self.shortname in suite.flaky_tests
        # True if the test was retried after it failed
        self.is_flaky_failure = False
        # True if the test was cancelled by a ctrl-c or timeout, so
        # shouldn't be retried, even if it is flaky
        self.is_cancelled = False
        self.env = dict(self.suite.base_env)
        Test._reset(self)

    def reset(self) -> None:
        """Reset this object, including all derived state."""
        for cls in reversed(self.__class__.__mro__):
            _reset = getattr(cls, '_reset', None)
            if _reset is not None:
                _reset(self)

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.success = False
        self.time_start: float = 0
        self.time_end: float = 0

    @abstractmethod
    async def run(self, options: argparse.Namespace) -> 'Test':
        pass

    @abstractmethod
    def print_summary(self) -> None:
        pass

    def check_log(self, trim: bool) -> None:
        """Check and trim logs and xml output for tests which have it"""
        if trim:
            self.log_filename.unlink()
        pass

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        assert not self.success
        xml_fail = ET.SubElement(xml_res, 'failure')
        xml_fail.text = "Test {} {} failed, check the log at {}".format(
            self.path,
            " ".join(self.args),
            self.log_filename)
        if self.log_filename.exists():
            system_out = ET.SubElement(xml_res, 'system-out')
            system_out.text = read_log(self.log_filename)


class UnitTest(Test):
    standard_args = shlex.split("--overprovisioned --unsafe-bypass-fsync 1 "
                                "--kernel-page-cache 1 "
                                "--blocked-reactor-notify-ms 2000000 --collectd 0 "
                                "--max-networking-io-control-blocks=100 ")

    def __init__(self, test_no: int, shortname: str, suite, args: str) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = path_to(self.mode, "test", suite.name, shortname.split('.')[0])
        self.args = shlex.split(args) + UnitTest.standard_args
        if self.mode == "coverage":
            self.env.update(coverage.env(self.path))

        UnitTest._reset(self)

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        pass

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options) -> Test:
        self.success = await run_test(self, options, env=self.env)
        logging.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self


TestPath = collections.namedtuple('TestPath', ['suite_name', 'test_name', 'case_name'])

class BoostTest(UnitTest):
    """A unit test which can produce its own XML output"""

    def __init__(self, test_no: int, shortname: str, suite, args: str,
                 casename: Optional[str], allows_compaction_groups : bool) -> None:
        boost_args = []
        if casename:
            shortname += '.' + casename
            boost_args += ['--run_test=' + casename]
        super().__init__(test_no, shortname, suite, args)
        self.xmlout = os.path.join(suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        boost_args += ['--report_level=no',
                       '--logger=HRF,test_suite:XML,test_suite,' + self.xmlout]
        boost_args += ['--catch_system_errors=no']  # causes undebuggable cores
        boost_args += ['--color_output=false']
        boost_args += ['--']
        self.args = boost_args + self.args
        self.casename = casename
        BoostTest._reset(self)
        self.__test_case_elements: list[ET.Element] = []
        self.allows_compaction_groups = allows_compaction_groups

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.__test_case_elements = []

    def get_test_cases(self) -> list[ET.Element]:
        if not self.__test_case_elements:
            self.__parse_logger()
        return self.__test_case_elements

    @staticmethod
    def test_path_of_element(test: ET.Element) -> TestPath:
        path = test.attrib['path']
        prefix, case_name = path.rsplit('::', 1)
        suite_name, test_name = prefix.split('.', 1)
        return TestPath(suite_name, test_name, case_name)

    def __parse_logger(self) -> None:
        def attach_path_and_mode(test):
            # attach the "path" to the test so we can group the tests by this string
            test_name = test.attrib['name']
            prefix = self.name.replace(os.path.sep, '.')
            test.attrib['path'] = f'{prefix}::{test_name}'
            test.attrib['mode'] = self.mode
            return test

        try:
            root = ET.parse(self.xmlout).getroot()
            # only keep the tests which actually ran, the skipped ones do not have
            # TestingTime tag in the corresponding TestCase tag.
            self.__test_case_elements = map(attach_path_and_mode,
                                            root.findall(".//TestCase[TestingTime]"))
            os.unlink(self.xmlout)
        except ET.ParseError as e:
            message = palette.crit(f"failed to parse XML output '{self.xmlout}': {e}")
            if e.msg.__contains__("no element found"):
                message = palette.crit(f"Empty testcase XML output, possibly caused by a crash in the cql_test_env.cc, "
                                       f"details: '{self.xmlout}': {e}")
            print(f"error: {self.name}: {message}")

    def check_log(self, trim: bool) -> None:
        self.__parse_logger()
        super().check_log(trim)

    async def run(self, options):
        if options.random_seed:
            self.args += ['--random-seed', options.random_seed]
        if self.allows_compaction_groups and options.x_log2_compaction_groups:
            self.args += [ "--x-log2-compaction-groups", str(options.x_log2_compaction_groups) ]
        return await super().run(options)

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        """Does not write junit report for Jenkins legacy reasons"""
        assert False


class CQLApprovalTest(Test):
    """Run a sequence of CQL commands against a standalone Scylla"""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        # Path to cql_repl driver, in the given build mode
        self.path = "pytest"
        self.cql = suite.suite_path / (self.shortname + ".cql")
        self.result = suite.suite_path / (self.shortname + ".result")
        self.tmpfile = os.path.join(suite.options.tmpdir, self.mode, self.uname + ".reject")
        self.reject = suite.suite_path / (self.shortname + ".reject")
        self.server_log: Optional[str] = None
        self.server_log_filename: Optional[pathlib.Path] = None
        CQLApprovalTest._reset(self)

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.is_before_test_ok = False
        self.is_executed_ok = False
        self.is_new = False
        self.is_after_test_ok = False
        self.is_equal_result = False
        self.summary = "not run"
        self.unidiff: Optional[str] = None
        self.server_log = None
        self.server_log_filename = None
        self.env: Dict[str, str] = dict()
        old_tmpfile = pathlib.Path(self.tmpfile)
        if old_tmpfile.exists():
            old_tmpfile.unlink()
        self.args = [
            "-s",  # don't capture print() inside pytest
            "test/pylib/cql_repl/cql_repl.py",
            "--input={}".format(self.cql),
            "--output={}".format(self.tmpfile),
            "--run_id={}".format(self.id),
            "--mode={}".format(self.mode),
        ]

    async def run(self, options: argparse.Namespace) -> Test:
        self.success = False
        self.summary = "failed"

        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        def set_summary(summary):
            self.summary = summary
            log_func = logger.info if self.success else logger.error
            log_func("Test %s %s", self.uname, summary)
            if self.server_log is not None:
                logger.info("Server log:\n%s", self.server_log)

        # TODO: consider dirty_on_exception=True
        async with (cm := self.suite.clusters.instance(False, logger)) as cluster:
            try:
                cluster.before_test(self.uname)
                logger.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
                self.args.insert(1, "--host={}".format(cluster.endpoint()))
                # If pre-check fails, e.g. because Scylla failed to start
                # or crashed between two tests, fail entire test.py
                self.is_before_test_ok = True
                cluster.take_log_savepoint()
                self.is_executed_ok = await run_test(self, options, env=self.env)
                cluster.after_test(self.uname, self.is_executed_ok)
                cm.dirty = cluster.is_dirty
                self.is_after_test_ok = True

                if not self.is_executed_ok:
                    set_summary("""returned non-zero return status.\n
Check test log at {}.""".format(self.log_filename))
                elif not os.path.isfile(self.tmpfile):
                    set_summary("failed: no output file")
                elif not os.path.isfile(self.result):
                    set_summary("failed: no result file")
                    self.is_new = True
                else:
                    self.is_equal_result = filecmp.cmp(self.result, self.tmpfile)
                    if not self.is_equal_result:
                        self.unidiff = format_unidiff(str(self.result), self.tmpfile)
                        set_summary("failed: test output does not match expected result")
                        assert self.unidiff is not None
                        logger.info("\n{}".format(palette.nocolor(self.unidiff)))
                    else:
                        self.success = True
                        set_summary("succeeded")
            except Exception as e:
                # Server log bloats the output if we produce it in all
                # cases. So only grab it when it's relevant:
                # 1) failed pre-check, e.g. start failure
                # 2) failed test execution.
                if not self.is_executed_ok:
                    self.server_log = cluster.read_server_log()
                    self.server_log_filename = cluster.server_log_filename()
                    if not self.is_before_test_ok:
                        set_summary("pre-check failed: {}".format(e))
                        print("Test {} {}".format(self.name, self.summary))
                        print("Server log  of the first server:\n{}".format(self.server_log))
                        # Don't try to continue if the cluster is broken
                        raise
                set_summary("failed: {}".format(e))
            finally:
                if os.path.exists(self.tmpfile):
                    if self.is_executed_ok and (self.is_new or not self.is_equal_result):
                        # Move the .reject file close to the .result file
                        # so that it's easy to analyze the diff or overwrite .result
                        # with .reject.
                        shutil.move(self.tmpfile, self.reject)
                    else:
                        pathlib.Path(self.tmpfile).unlink()

        return self

    def print_summary(self) -> None:
        print("Test {} ({}) {}".format(palette.path(self.name), self.mode,
                                       self.summary))
        if not self.is_executed_ok:
            print(read_log(self.log_filename))
            if self.server_log is not None:
                print("Server log of the first server:")
                print(self.server_log)
        elif not self.is_equal_result and self.unidiff:
            print(self.unidiff)

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        assert not self.success
        xml_fail = ET.SubElement(xml_res, 'failure')
        xml_fail.text = self.summary
        if not self.is_executed_ok:
            if self.log_filename.exists():
                system_out = ET.SubElement(xml_res, 'system-out')
                system_out.text = read_log(self.log_filename)
            if self.server_log_filename:
                system_err = ET.SubElement(xml_res, 'system-err')
                system_err.text = read_log(self.server_log_filename)
        elif self.unidiff:
            system_out = ET.SubElement(xml_res, 'system-out')
            system_out.text = palette.nocolor(self.unidiff)


class RunTest(Test):
    """Run tests in a directory started by a run script"""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = suite.suite_path / shortname
        self.xmlout = os.path.join(suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.args = [
            "--junit-xml={}".format(self.xmlout),
            "-o",
            "junit_suite_name={}".format(self.suite.name)
        ]
        RunTest._reset(self)

    def _reset(self):
        """Reset the test before a retry, if it is retried as flaky"""
        pass

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options: argparse.Namespace) -> Test:
        # This test can and should be killed gently, with SIGTERM, not with SIGKILL
        self.success = await run_test(self, options, gentle_kill=True, env=self.suite.scylla_env)
        logging.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self


class PythonTest(Test):
    """Run a pytest collection of cases against a standalone Scylla"""

    def __init__(self, test_no: int, shortname: str, casename: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = "pytest"
        self.casename = casename
        self.xmlout = os.path.join(self.suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.server_log: Optional[str] = None
        self.server_log_filename: Optional[pathlib.Path] = None
        PythonTest._reset(self)

    def _prepare_pytest_params(self, options: argparse.Namespace):
        self.args = [
            "-s",  # don't capture print() output inside pytest
            "--log-level=DEBUG",   # Capture logs
            "-o",
            "junit_family=xunit2",
            "-o",
            "junit_suite_name={}".format(self.suite.name),
            "--junit-xml={}".format(self.xmlout),
            "-rs",
            "--run_id={}".format(self.id),
            "--mode={}".format(self.mode)
        ]
        if options.markers:
            self.args.append(f"-m={options.markers}")

            # https://docs.pytest.org/en/7.1.x/reference/exit-codes.html
            no_tests_selected_exit_code = 5
            self.valid_exit_codes = [0, no_tests_selected_exit_code]

        arg = str(self.suite.suite_path / (self.shortname + ".py"))
        if self.casename is not None:
            arg += '::' + self.casename
        self.args.append(arg)

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.server_log = None
        self.server_log_filename = None
        self.is_before_test_ok = False
        self.is_after_test_ok = False

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))
        if self.server_log is not None:
            print("Server log of the first server:")
            print(self.server_log)

    async def run(self, options: argparse.Namespace) -> Test:

        self._prepare_pytest_params(options)

        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        cluster = await self.suite.clusters.get(logger)
        try:
            cluster.before_test(self.uname)
            prepare_cql = self.suite.cfg.get("prepare_cql", None)
            if prepare_cql:
                next(iter(cluster.running.values())).control_connection.execute(prepare_cql)
            logger.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
            self.args.insert(0, "--host={}".format(cluster.endpoint()))
            self.is_before_test_ok = True
            cluster.take_log_savepoint()
            status = await run_test(self, options, env=self.suite.scylla_env)
            if self.shortname in self.suite.dirties_cluster:
                cluster.is_dirty = True
            cluster.after_test(self.uname, status)
            self.is_after_test_ok = True
            self.success = status
        except Exception as e:
            self.server_log = cluster.read_server_log()
            self.server_log_filename = cluster.server_log_filename()
            if not self.is_before_test_ok:
                print("Test {} pre-check failed: {}".format(self.name, str(e)))
                print("Server log of the first server:\n{}".format(self.server_log))
                logger.info(f"Discarding cluster after failed start for test %s...", self.name)
            elif not self.is_after_test_ok:
                print("Test {} post-check failed: {}".format(self.name, str(e)))
                print("Server log of the first server:\n{}".format(self.server_log))
                logger.info(f"Discarding cluster after failed test %s...", self.name)
        await self.suite.clusters.put(cluster, is_dirty=cluster.is_dirty)
        logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        super().write_junit_failure_report(xml_res)
        if self.server_log_filename is not None:
            system_err = ET.SubElement(xml_res, 'system-err')
            system_err.text = read_log(self.server_log_filename)


class TopologyTest(PythonTest):
    """Run a pytest collection of cases against Scylla clusters handling topology changes"""
    status: bool

    def __init__(self, test_no: int, shortname: str, casename: str, suite) -> None:
        super().__init__(test_no, shortname, casename, suite)

    async def run(self, options: argparse.Namespace) -> Test:

        self._prepare_pytest_params(options)

        test_path = os.path.join(self.suite.options.tmpdir, self.mode)
        async with get_cluster_manager(self.mode + '/' + self.uname, self.suite.clusters, test_path) as manager:
            self.args.insert(0, "--run_id={}".format(self.id))
            self.args.insert(0, "--tmpdir={}".format(options.tmpdir))
            self.args.insert(0, "--mode={}".format(self.mode))
            self.args.insert(0, "--manager-api={}".format(manager.sock_path))
            if options.artifacts_dir_url:
                self.args.insert(0, "--artifacts_dir_url={}".format(options.artifacts_dir_url))

            try:
                # Note: start manager here so cluster (and its logs) is available in case of failure
                await manager.start()
                self.success = await run_test(self, options)
            except Exception as e:
                self.server_log = manager.cluster.read_server_log()
                self.server_log_filename = manager.cluster.server_log_filename()
                if not manager.is_before_test_ok:
                    print("Test {} pre-check failed: {}".format(self.name, str(e)))
                    print("Server log of the first server:\n{}".format(self.server_log))
                    # Don't try to continue if the cluster is broken
                    raise
            manager.logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self


class ToolTest(Test):
    """Run a collection of pytest test cases

    That do not need a scylla cluster set-up for them."""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        launcher = self.suite.cfg.get("launcher", "pytest")
        self.path = launcher.split(maxsplit=1)[0]
        self.xmlout = os.path.join(self.suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        ToolTest._reset(self)

    def _prepare_pytest_params(self, options: argparse.Namespace):
        launcher = self.suite.cfg.get("launcher", "pytest")
        self.args = launcher.split()[1:]
        self.args += [
            "-s",  # don't capture print() output inside pytest
            "--log-level=DEBUG",   # Capture logs
            "-o",
            "junit_family=xunit2",
            "--junit-xml={}".format(self.xmlout),
            "--mode={}".format(self.mode),
            "--run_id={}".format(self.id)
        ]
        if options.markers:
            self.args.append(f"-m={options.markers}")

            # https://docs.pytest.org/en/7.1.x/reference/exit-codes.html
            no_tests_selected_exit_code = 5
            self.valid_exit_codes = [0, no_tests_selected_exit_code]
        self.args.append(str(self.suite.suite_path / (self.shortname + ".py")))

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        pass

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))

    async def run(self, options: argparse.Namespace) -> Test:
        self._prepare_pytest_params(options)

        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        self.success = await run_test(self, options)
        logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        super().write_junit_failure_report(xml_res)


class TabularConsoleOutput:
    """Print test progress to the console"""

    def __init__(self, verbose: bool, test_count: int) -> None:
        self.verbose = verbose
        self.test_count = test_count
        self.print_newline = False
        self.last_test_no = 0
        self.last_line_len = 1

    def print_start_blurb(self) -> None:
        print("="*80)
        print("{:10s} {:^8s} {:^7s} {:8s} {}".format("[N/TOTAL]", "SUITE", "MODE", "RESULT", "TEST"))
        print("-"*78)

    def print_end_blurb(self) -> None:
        if self.print_newline:
            print("")
        print("-"*78)

    def print_progress(self, test: Test) -> None:
        self.last_test_no += 1
        status = ""
        if test.success:
            logging.debug("Test {} is flaky {}".format(test.uname,
                                                       test.is_flaky_failure))
            if test.is_flaky_failure:
                status = palette.warn("[ FLKY ]")
            else:
                status = palette.ok("[ PASS ]")
        else:
            status = palette.fail("[ FAIL ]")
        msg = "{:10s} {:^8s} {:^7s} {:8s} {}".format(
            "[{}/{}]".format(self.last_test_no, self.test_count),
            test.suite.name, test.mode[:7],
            status,
            test.uname
        )
        if not self.verbose:
            if test.success:
                print("\r" + " " * self.last_line_len, end="")
                self.last_line_len = len(msg)
                print("\r" + msg, end="")
                self.print_newline = True
            else:
                if self.print_newline:
                    print("")
                print(msg)
                self.print_newline = False
        else:
            msg += " {:.2f}s".format(test.time_end - test.time_start)
            print(msg)


async def run_test(test: Test, options: argparse.Namespace, gentle_kill=False, env=dict()) -> bool:
    """Run test program, return True if success else False"""

    with test.log_filename.open("wb") as log:

        def report_error(error):
            msg = "=== TEST.PY SUMMARY START ===\n"
            msg += "{}\n".format(error)
            msg += "=== TEST.PY SUMMARY END ===\n"
            log.write(msg.encode(encoding="UTF-8"))
        process = None
        stdout = None
        logging.info("Starting test %s: %s %s", test.uname, test.path, " ".join(test.args))
        UBSAN_OPTIONS = [
            "halt_on_error=1",
            "abort_on_error=1",
            f"suppressions={os.getcwd()}/ubsan-suppressions.supp",
            os.getenv("UBSAN_OPTIONS"),
        ]
        ASAN_OPTIONS = [
            "disable_coredump=0",
            "abort_on_error=1",
            "detect_stack_use_after_return=1",
            os.getenv("ASAN_OPTIONS"),
        ]
        try:
            log.write("=== TEST.PY STARTING TEST {} ===\n".format(test.uname).encode(encoding="UTF-8"))
            log.write("export UBSAN_OPTIONS='{}'\n".format(
                ":".join(filter(None, UBSAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("export ASAN_OPTIONS='{}'\n".format(
                ":".join(filter(None, ASAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("{} {}\n".format(test.path, " ".join(test.args)).encode(encoding="UTF-8"))
            log.write("=== TEST.PY TEST {} OUTPUT ===\n".format(test.uname).encode(encoding="UTF-8"))
            log.flush()
            test.time_start = time.time()
            test.time_end = 0

            path = test.path
            args = test.args
            if options.cpus:
                path = 'taskset'
                args = ['-c', options.cpus, test.path, *test.args]
            process = await asyncio.create_subprocess_exec(
                path, *args,
                stderr=log,
                stdout=log,
                env=dict(os.environ,
                         UBSAN_OPTIONS=":".join(filter(None, UBSAN_OPTIONS)),
                         ASAN_OPTIONS=":".join(filter(None, ASAN_OPTIONS)),
                         # TMPDIR env variable is used by any seastar/scylla
                         # test for directory to store test temporary data.
                         TMPDIR=os.path.join(options.tmpdir, test.mode),
                         SCYLLA_TEST_ENV='yes',
                         **env,
                         ),
                preexec_fn=os.setsid,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), options.timeout)
            test.time_end = time.time()
            if process.returncode not in test.valid_exit_codes:
                report_error('Test exited with code {code}\n'.format(code=process.returncode))
                return False
            try:
                test.check_log(not options.save_log_on_success)
            except Exception as e:
                print("")
                print(test.name + ": " + palette.crit("failed to parse XML output: {}".format(e)))
                return False
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError) as e:
            test.is_cancelled = True
            if process is not None:
                if gentle_kill:
                    process.terminate()
                else:
                    process.kill()
                stdout, _ = await process.communicate()
            if isinstance(e, asyncio.TimeoutError):
                report_error("Test timed out")
            elif isinstance(e, asyncio.CancelledError):
                print(test.shortname, end=" ")
                report_error("Test was cancelled: the parent process is exiting")
        except Exception as e:
            report_error("Failed to run the test:\n{e}".format(e=e))
    return False


def setup_signal_handlers(loop, signaled) -> None:

    async def shutdown(loop, signo, signaled):
        print("\nShutdown requested... Aborting tests:"),
        signaled.signo = signo
        signaled.set()

    # Use a lambda to avoid creating a coroutine until
    # the signal is delivered to the loop - otherwise
    # the coroutine will be dangling when the loop is over,
    # since it's never going to be invoked
    for signo in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(signo, lambda: asyncio.create_task(shutdown(loop, signo, signaled)))


def parse_cmd_line() -> argparse.Namespace:
    """ Print usage and process command line options. """

    parser = argparse.ArgumentParser(description="Scylla test runner")
    parser.add_argument(
        "name",
        nargs="*",
        action="store",
        help="""Can be empty. List of test names, to look for in
                suites. Each name is used as a substring to look for in the
                path to test file, e.g. "mem" will run all tests that have
                "mem" in their name in all suites, "boost/mem" will only enable
                tests starting with "mem" in "boost" suite, and
                "boost/memtable_test::test_hash_is_cached" to narrow down to
                a certain test case. Default: run all tests in all suites.""",
    )
    parser.add_argument(
        "--tmpdir",
        action="store",
        default="testlog",
        help="""Path to temporary test data and log files. The data is
        further segregated per build mode. Default: ./testlog.""",
    )
    parser.add_argument('--mode', choices=all_modes.keys(), action="append", dest="modes",
                        help="Run only tests for given build mode(s)")
    parser.add_argument('--repeat', action="store", default="1", type=int,
                        help="number of times to repeat test execution")
    parser.add_argument('--timeout', action="store", default="24000", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument('--save-log-on-success', "-s", default=False,
                        dest="save_log_on_success", action="store_true",
                        help="Save test log output on success.")
    parser.add_argument('--list', dest="list_tests", action="store_true", default=False,
                        help="Print list of tests instead of executing them")
    parser.add_argument('--skip',
                        dest="skip_patterns", action="append",
                        help="Skip tests which match the provided pattern")
    parser.add_argument('--no-parallel-cases', dest="parallel_cases", action="store_false", default=True,
                        help="Do not run individual test cases in parallel")
    parser.add_argument('--cpus', action="store",
                        help="Run the tests on those CPUs only (in taskset"
                        " acceptable format). Consider using --jobs too")
    parser.add_argument('--log-level', action="store",
                        help="Log level for Python logging module. The log "
                        "is in {tmpdir}/test.py.log. Default: INFO",
                        default="INFO",
                        choices=["CRITICAL", "ERROR", "WARNING", "INFO",
                                 "DEBUG"],
                        dest="log_level")
    parser.add_argument('--markers', action='store', metavar='MARKEXPR',
                        help="Only run tests that match the given mark expression. The syntax is the same "
                             "as in pytest, for example: --markers 'mark1 and not mark2'. The parameter "
                             "is only supported by python tests for now, other tests ignore it. "
                             "By default, the marker filter is not applied and all tests will be run without exception."
                             "To exclude e.g. slow tests you can write --markers 'not slow'.")
    parser.add_argument('--coverage', action = 'store_true', default = False,
                        help="When running code instrumented with coverage support"
                             "Will route the profiles to `tmpdir`/mode/coverage/`suite` and post process them in order to generate "
                             "lcov file per suite, lcov file per mode, and an lcov file for the entire run, "
                             "The lcov files can eventually be used for generating coverage reports")
    parser.add_argument("--coverage-mode",action = 'append', type = str, dest = "coverage_modes",
                        help = "Collect and process coverage only for the modes specified. implies: --coverage, default: All built modes")
    parser.add_argument("--coverage-keep-raw",action = 'store_true',
                        help = "Do not delete llvm raw profiles when processing coverage reports.")
    parser.add_argument("--coverage-keep-indexed",action = 'store_true',
                        help = "Do not delete llvm indexed profiles when processing coverage reports.")
    parser.add_argument("--coverage-keep-lcovs",action = 'store_true',
                        help = "Do not delete intermediate lcov traces when processing coverage reports.")
    parser.add_argument("--artifacts_dir_url", action='store', type=str, default=None, dest="artifacts_dir_url",
                        help="Provide the URL to artifacts directory to generate the link to failed tests directory "
                             "with logs")
    parser.add_argument("--cluster-pool-size", action="store", default=None, type=int,
                        help="Set the pool_size for PythonTest and its descendants. Alternatively environment variable "
                             "CLUSTER_POOL_SIZE can be used to achieve the same")
    scylla_additional_options = parser.add_argument_group('Additional options for Scylla tests')
    scylla_additional_options.add_argument('--x-log2-compaction-groups', action="store", default="0", type=int,
                             help="Controls number of compaction groups to be used by Scylla tests. Value of 3 implies 8 groups.")
    scylla_additional_options.add_argument('--extra-scylla-cmdline-options', action="store", default=[], type=str,
                                           help="Passing extra scylla cmdline options for all tests. Options should be space separated:"
                                                "'--logger-log-level raft=trace --default-log-level error'")

    boost_group = parser.add_argument_group('boost suite options')
    boost_group.add_argument('--random-seed', action="store",
                             help="Random number generator seed to be used by boost tests")

    args = parser.parse_args()

    if not args.jobs:
        if not args.cpus:
            nr_cpus = multiprocessing.cpu_count()
        else:
            nr_cpus = int(subprocess.check_output(
                ['taskset', '-c', args.cpus, 'python3', '-c',
                 'import os; print(len(os.sched_getaffinity(0)))']))

        cpus_per_test_job = 1
        sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        testmem = 6e9 if os.sysconf('SC_PAGE_SIZE') > 4096 else 2e9
        default_num_jobs_mem = ((sysmem - 4e9) // testmem)
        args.jobs = min(default_num_jobs_mem, nr_cpus // cpus_per_test_job)

    if not output_is_a_tty:
        args.verbose = True

    if not args.modes:
        try:
            out = ninja('mode_list')
            # [1/1] List configured modes
            # debug release dev
            args.modes = re.sub(r'.* List configured modes\n(.*)\n', r'\1',
                                out, 1, re.DOTALL).split("\n")[-1].split(' ')
        except Exception:
            print(palette.fail("Failed to read output of `ninja mode_list`: please run ./configure.py first"))
            raise

    if not args.coverage_modes and args.coverage:
        args.coverage_modes = list(args.modes)
        if "coverage" in args.coverage_modes:
            args.coverage_modes.remove("coverage")
        if not args.coverage_modes:
            args.coverage = False
    elif args.coverage_modes:
        if "coverage" in args.coverage_modes:
            raise RuntimeError("'coverage' mode is not allowed in --coverage-mode")
        missing_coverage_modes = set(args.coverage_modes).difference(set(args.modes))
        if len(missing_coverage_modes) > 0:
            raise RuntimeError(f"The following modes weren't built or ran (using the '--mode' option): {missing_coverage_modes}")
        args.coverage = True
    def prepare_dir(dirname, pattern):
        # Ensure the dir exists
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        # Remove old artifacts
        for p in glob.glob(os.path.join(dirname, pattern), recursive=True):
            pathlib.Path(p).unlink()

    args.tmpdir = os.path.abspath(args.tmpdir)
    prepare_dir(args.tmpdir, "*.log")

    for mode in args.modes:
        prepare_dir(os.path.join(args.tmpdir, mode), "*.log")
        prepare_dir(os.path.join(args.tmpdir, mode), "*.reject")
        prepare_dir(os.path.join(args.tmpdir, mode, "xml"), "*.xml")
        shutil.rmtree(os.path.join(args.tmpdir, mode, "failed_test"), ignore_errors=True)
        prepare_dir(os.path.join(args.tmpdir, mode, "failed_test"), "*")
        prepare_dir(os.path.join(args.tmpdir, mode, "allure"), "*.xml")

    # Get the list of tests configured by configure.py
    try:
        out = ninja('unit_test_list')
        # [1/1] List configured unit tests
        args.tests = set(re.sub(r'.* List configured unit tests\n(.*)\n', r'\1', out, 1, re.DOTALL).split("\n"))
    except Exception:
        print(palette.fail("Failed to read output of `ninja unit_test_list`: please run ./configure.py first"))
        raise

    if args.extra_scylla_cmdline_options:
        args.extra_scylla_cmdline_options = args.extra_scylla_cmdline_options.split()

    return args


async def find_tests(options: argparse.Namespace) -> None:

    for f in glob.glob(os.path.join("test", "*")):
        if os.path.isdir(f) and os.path.isfile(os.path.join(f, "suite.yaml")):
            for mode in options.modes:
                suite = TestSuite.opt_create(f, options, mode)
                await suite.add_test_list()

    if not TestSuite.test_count():
        if len(options.name):
            print("Test {} not found".format(palette.path(options.name[0])))
            sys.exit(1)
        else:
            print(palette.warn("No tests found. Please enable tests in ./configure.py first."))
            sys.exit(0)

    logging.info("Found %d tests, repeat count is %d, starting %d concurrent jobs",
                 TestSuite.test_count(), options.repeat, options.jobs)
    print("Found {} tests.".format(TestSuite.test_count()))


async def run_all_tests(signaled: asyncio.Event, options: argparse.Namespace) -> None:
    console = TabularConsoleOutput(options.verbose, TestSuite.test_count())
    signaled_task = asyncio.create_task(signaled.wait())
    pending = set([signaled_task])

    async def cancel(pending):
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        print("... done.")
        raise asyncio.CancelledError

    async def reap(done, pending, signaled):
        nonlocal console
        if signaled.is_set():
            await cancel(pending)
        for coro in done:
            result = coro.result()
            if isinstance(result, bool):
                continue    # skip signaled task result
            console.print_progress(result)

    ms = MinioServer(options.tmpdir, '127.0.0.1', LogPrefixAdapter(logging.getLogger('minio'), {'prefix': 'minio'}))
    await ms.start()
    TestSuite.artifacts.add_exit_artifact(None, ms.stop)

    console.print_start_blurb()
    try:
        TestSuite.artifacts.add_exit_artifact(None, TestSuite.hosts.cleanup)
        for test in TestSuite.all_tests():
            # +1 for 'signaled' event
            if len(pending) > options.jobs:
                # Wait for some task to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                await reap(done, pending, signaled)
            pending.add(asyncio.create_task(test.suite.run(test, options)))
        # Wait & reap ALL tasks but signaled_task
        # Do not use asyncio.ALL_COMPLETED to print a nice progress report
        while len(pending) > 1:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await reap(done, pending, signaled)

    except asyncio.CancelledError:
        return
    finally:
        await TestSuite.artifacts.cleanup_before_exit()

    console.print_end_blurb()


def read_log(log_filename: pathlib.Path) -> str:
    """Intelligently read test log output"""
    try:
        with log_filename.open("r") as log:
            msg = log.read()
            return msg if len(msg) else "===Empty log output==="
    except FileNotFoundError:
        return "===Log {} not found===".format(log_filename)
    except OSError as e:
        return "===Error reading log {}===".format(e)


def print_summary(failed_tests, options: argparse.Namespace) -> None:
    rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_used = rusage.ru_stime + rusage.ru_utime
    cpu_available = (time.monotonic() - launch_time) * multiprocessing.cpu_count()
    utilization = cpu_used / cpu_available
    print(f"CPU utilization: {utilization*100:.1f}%")
    if failed_tests:
        print("The following test(s) have failed: {}".format(
            palette.path(" ".join([t.name for t in failed_tests]))))
        if options.verbose:
            for test in failed_tests:
                test.print_summary()
                print("-"*78)
        print("Summary: {} of the total {} tests failed".format(
            len(failed_tests), TestSuite.test_count()))


def format_unidiff(fromfile: str, tofile: str) -> str:
    with open(fromfile, "r") as frm, open(tofile, "r") as to:
        buf = StringIO()
        diff = difflib.unified_diff(
            frm.readlines(),
            to.readlines(),
            fromfile=fromfile,
            tofile=tofile,
            fromfiledate=time.ctime(os.stat(fromfile).st_mtime),
            tofiledate=time.ctime(os.stat(tofile).st_mtime),
            n=10)           # Number of context lines

        for i, line in enumerate(diff):
            if i > 60:
                break
            if line.startswith('+'):
                line = palette.diff_in(line)
            elif line.startswith('-'):
                line = palette.diff_out(line)
            elif line.startswith('@'):
                line = palette.diff_mark(line)
            buf.write(line)
        return buf.getvalue()


def write_junit_report(tmpdir: str, mode: str) -> None:
    junit_filename = os.path.join(tmpdir, mode, "xml", "junit.xml")
    total = 0
    failed = 0
    xml_results = ET.Element("testsuite", name="non-boost tests", errors="0")
    for suite in TestSuite.suites.values():
        for test in suite.junit_tests():
            if test.mode != mode:
                continue
            total += 1
            test_time = f"{test.time_end - test.time_start:.3f}"
            # add the suite name to disambiguate tests named "run"
            xml_res = ET.SubElement(xml_results, 'testcase',
                                    name="{}.{}.{}.{}".format(test.suite.name, test.shortname, mode, test.id),
                                    time=test_time)
            if test.success is True:
                continue
            failed += 1
            test.write_junit_failure_report(xml_res)
    if total == 0:
        return
    xml_results.set("tests", str(total))
    xml_results.set("failures", str(failed))
    with open(junit_filename, "w") as f:
        ET.ElementTree(xml_results).write(f, encoding="unicode")


def summarize_boost_tests(tests):
    # in case we run a certain test multiple times
    # - if any of the runs failed, the test is considered failed, and
    #   the last failed run is returned.
    # - otherwise, the last successful run is returned
    failed_test = None
    passed_test = None
    num_failed_tests = collections.defaultdict(int)
    num_passed_tests = collections.defaultdict(int)
    for test in tests:
        error = None
        for tag in ['Error', 'FatalError', 'Exception']:
            error = test.find(tag)
            if error is not None:
                break
        mode = test.attrib['mode']
        if error is None:
            passed_test = test
            num_passed_tests[mode] += 1
        else:
            failed_test = test
            num_failed_tests[mode] += 1

    if failed_test is not None:
        test = failed_test
    else:
        test = passed_test

    num_failed = sum(num_failed_tests.values())
    num_passed = sum(num_passed_tests.values())
    num_total = num_failed + num_passed
    if num_total == 1:
        return test
    if num_failed == 0:
        return test
    # we repeated this test for multiple times.
    #
    # Boost::test's XML logger schema does not allow us to put text directly in a
    # TestCase tag, so create a dummy Message tag in the TestCase for carrying the
    # summary. and the schema requires that the tags should be listed in following order:
    # 1. TestSuite
    # 2. Info
    # 3. Error
    # 3. FatalError
    # 4. Message
    # 5. Exception
    # 6. Warning
    # and both "file" and "line" are required in an "Info" tag, so appease it. assuming
    # there is no TestSuite under tag TestCase, we always add Info as the first subelements
    if num_passed == 0:
        message = ET.Element('Info', file=test.attrib['file'], line=test.attrib['line'])
        message.text = f'The test failed {num_failed}/{num_total} times'
        test.insert(0, message)
    else:
        message = ET.Element('Info', file=test.attrib['file'], line=test.attrib['line'])
        modes = ', '.join(f'{mode}={n}' for mode, n in num_failed_tests.items())
        message.text = f'failed: {modes}'
        test.insert(0, message)

        message = ET.Element('Info', file=test.attrib['file'], line=test.attrib['line'])
        modes = ', '.join(f'{mode}={n}' for mode, n in num_passed_tests.items())
        message.text = f'passed: {modes}'
        test.insert(0, message)

        message = ET.Element('Info', file=test.attrib['file'], line=test.attrib['line'])
        message.text = f'{num_failed} out of {num_total} times failed.'
        test.insert(0, message)
    return test


def write_consolidated_boost_junit_xml(tmpdir: str, mode: str) -> str:
    # collects all boost tests sorted by their full names
    boost_tests = itertools.chain.from_iterable(suite.boost_tests()
                                                for suite in TestSuite.suites.values())
    test_cases = itertools.chain.from_iterable(test.get_test_cases()
                                               for test in boost_tests)
    test_cases = sorted(test_cases, key=BoostTest.test_path_of_element)

    xml = ET.Element("TestLog")
    for full_path, tests in itertools.groupby(
            test_cases,
            key=BoostTest.test_path_of_element):
        # dedup the tests with the same name, so only the representative one is
        # preserved
        test_case = summarize_boost_tests(tests)
        test_case.attrib.pop('path')
        test_case.attrib.pop('mode')

        suite_name, test_name, _ = full_path
        suite = xml.find(f"./TestSuite[@name='{suite_name}']")
        if suite is None:
            suite = ET.SubElement(xml, 'TestSuite', name=suite_name)
        test = suite.find(f"./TestSuite[@name='{test_name}']")
        if test is None:
            test = ET.SubElement(suite, 'TestSuite', name=test_name)
        test.append(test_case)
    et = ET.ElementTree(xml)
    xunit_file = f'{tmpdir}/{mode}/xml/boost.xunit.xml'
    et.write(xunit_file, encoding='unicode')
    return xunit_file


def boost_to_junit(boost_xml, junit_xml):
    boost_root = ET.parse(boost_xml).getroot()
    junit_root = ET.Element('testsuites')

    def parse_tag_output(test_case_element: ET.Element, tag_output: str) -> str:
        text_template = '''
            [{level}] - {level_message}
            [FILE] - {file_name}
            [LINE] - {line_number}
            '''
        text = ''
        tag_outputs = test_case_element.findall(tag_output)
        if tag_outputs:
            for tag_output in tag_outputs:
                text += text_template.format(level='Info', level_message=tag_output.text,
                                             file_name=tag_output.get('file'), line_number=tag_output.get('line'))
        return text

    # report produced {write_consolidated_boost_junit_xml} have the nested structure suite_boost -> [suite1, suite2, ...]
    # so we are excluding the upper suite with name boost
    for test_suite in boost_root.findall('./TestSuite/TestSuite'):
        suite_time = 0.0
        suite_test_total = 0
        suite_test_fails_number = 0

        junit_test_suite = ET.SubElement(junit_root, 'testsuite')
        junit_test_suite.attrib['name'] = test_suite.attrib['name']

        test_cases = test_suite.findall('TestCase')
        for test_case in test_cases:
            # convert the testing time: boost uses microseconds and Junit uses seconds
            test_case_time = int(test_case.find('TestingTime').text) / 1_000_000
            suite_time += test_case_time
            suite_test_total += 1

            junit_test_case = ET.SubElement(junit_test_suite, 'testcase')
            junit_test_case.set('name', test_case.get('name'))
            junit_test_case.set('time', str(test_case_time))
            junit_test_case.set('file', test_case.get('file'))
            junit_test_case.set('line', test_case.get('line'))

            system_out = ET.SubElement(junit_test_case, 'system-out')
            system_out.text = ''
            for tag in ['Info', 'Message', 'Exception']:
                output = parse_tag_output(test_case, tag)
                if output:
                    system_out.text += output

        junit_test_suite.set('tests', str(suite_test_total))
        junit_test_suite.set('time', str(suite_time))
        junit_test_suite.set('failures', str(suite_test_fails_number))
    ET.ElementTree(junit_root).write(junit_xml, encoding='UTF-8')


def open_log(tmpdir: str, log_file_name: str, log_level: str) -> None:
    pathlib.Path(tmpdir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(tmpdir, log_file_name),
        filemode="w",
        level=log_level,
        format="%(asctime)s.%(msecs)03d %(levelname)s> %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.critical("Started %s", " ".join(sys.argv))


async def main() -> int:

    options = parse_cmd_line()

    open_log(options.tmpdir, f"test.py.{'-'.join(options.modes)}.log", options.log_level)

    await find_tests(options)
    if options.list_tests:
        print('\n'.join([f"{t.suite.mode:<8} {type(t.suite).__name__[:-9]:<11} {t.name}"
                         for t in TestSuite.all_tests()]))
        return 0

    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_running_loop(), signaled)

    try:
        await run_all_tests(signaled, options)
    except Exception as e:
        print(palette.fail(e))
        raise

    if signaled.is_set():
        return -signaled.signo      # type: ignore

    failed_tests = [t for t in TestSuite.all_tests() if t.success is not True]

    print_summary(failed_tests, options)

    for mode in options.modes:
        junit_file = f"{options.tmpdir}/{mode}/allure/boost.junit.xml"
        write_junit_report(options.tmpdir, mode)
        xunit_file = write_consolidated_boost_junit_xml(options.tmpdir, mode)
        boost_to_junit(xunit_file, junit_file)

    if 'coverage' in options.modes:
        coverage.generate_coverage_report(path_to("coverage", "tests"))

    if options.coverage:
        await process_coverage(options)

    # Note: failure codes must be in the ranges 0-124, 126-127,
    #       to cooperate with git bisect's expectations
    return 0 if not failed_tests else 1

async def process_coverage(options):
    total_processing_time = time.time()
    logger = LogPrefixAdapter(logging.getLogger("coverage"), {'prefix' : 'coverage'})
    modes_for_coverage = options.coverage_modes
    # use about 75% of the machine's processing power.
    concurrency = max(int(multiprocessing.cpu_count() * 0.75), 1)
    logger.info(f"Processing coverage information for modes: {modes_for_coverage}, using {concurrency} cpus")
    semaphore = asyncio.Semaphore(concurrency)
    build_paths = [pathlib.Path(f"build/{mode}") for mode in modes_for_coverage]
    paths_for_id_search = [bp / p for bp, p in itertools.product(build_paths, ["scylla", "test", "seastar"])]
    logger.info("Getting binary ids for coverage conversion...")
    files_to_ids_map = await coverage_utils.get_binary_ids_map(paths = paths_for_id_search,
                                                               filter = coverage_utils.PROFILED_ELF_TYPES,
                                                               semaphore = semaphore,
                                                               logger = logger)
    logger.debug(f"Binary ids map is: {files_to_ids_map}")
    logger.info("Done getting binary ids for coverage conversion")
    # get the suits that have actually been ran
    suits_to_exclude = ["pylib_test", "nodetool"]
    sources_to_exclude = [line for line in open("coverage_excludes.txt", 'r').read().split('\n') if line and not line.startswith('#')]
    ran_suites = list({test.suite for test in TestSuite.all_tests() if test.suite.need_coverage()})

    def suite_coverage_path(suite) -> pathlib.Path:
        return pathlib.Path(suite.options.tmpdir) / suite.mode / 'coverage' / suite.name

    def pathsize(path : pathlib.Path):
        if path.is_file():
            return os.path.getsize(path)
        elif path.is_dir():
            return sum([os.path.getsize(f) for f in path.glob("**/*") if f.is_file()])
        else:
            return 0
    class Stats:
        def __init__(self, name = "", size = 0, time = 0) -> None:
            self.name = name
            self.size = size
            self.time = time
        def __add__(self, other):
            return Stats(self.name,
                         size = self.size + other.size,
                         time = self.time + other.time)
        def __str__(self):
            name = f"{self.name} - " if self.name else ""
            fields = []
            if self.size:
                fields.append(f"size: {humanfriendly.format_size(self.size)}")
            if self.time:
                fields.append(f"time: {humanfriendly.format_timespan(self.time)}")
            fields = ', '.join(fields)
            return f"{name}{fields}"
        @property
        def asstring(self):
            return str(self)

    # a nested map of: mode -> suite -> unified_coverage_file
    suits_trace_files = {}
    stats = treelib.Tree()

    RAW_PROFILE_STATS = "raw profiles"
    INDEXED_PROFILE_STATS = "indexed profiles"
    LCOV_CONVERSION_STATS = "lcov conversion"
    LCOV_SUITES_MEREGE_STATS = "lcov per suite merge"
    LCOV_MODES_MERGE_STATS = "lcov merge for mode"
    LCOV_MERGE_ALL_STATS = "lcov merge all stats"
    ROOT_NODE = stats.create_node(tag = time.time(),
                                  identifier = "root",
                                  data = Stats("Coverage Processing Stats", 0, 0))

    for suite in list(ran_suites):
        coverage_path = suite_coverage_path(suite)
        if not coverage_path.exists():
            logger.warning(f"Coverage dir for suite '{suite.name}' in mode '{suite.mode}' wasn't found, common reasons:\n\t"
                "1. The suite doesn't use any instrumented binaries.\n\t"
                "2. The binaries weren't compiled with coverage instrumentation.")
            continue

        # 1. Transform every suite raw profiles into indexed profiles
        raw_profiles = list(coverage_path.glob("*.profraw"))
        if len(raw_profiles) == 0:
            logger.warning(f"Couldn't find any raw profiles for suite '{suite.name}' in mode '{suite.mode}' ({coverage_path}):\n\t"
                "1. The binaries are killed instead of terminating which bypasses profile dump.\n\t"
                "2. The suite tempres with the LLVM_PROFILE_FILE which causes the profile to be dumped\n\t"
                "   to somewhere else.")
            continue
        mode_stats = stats.get_node(suite.mode)
        if not mode_stats:
            mode_stats = stats.create_node(tag = time.time(),
                                           identifier = suite.mode,
                                           parent = ROOT_NODE,
                                           data = Stats(f"{suite.mode} mode processing stats", 0, 0))

        raw_stats_node = stats.get_node(mode_stats.identifier + RAW_PROFILE_STATS)
        if not raw_stats_node:
            raw_stats_node = stats.create_node(tag = time.time(),
                                               identifier = mode_stats.identifier + RAW_PROFILE_STATS,
                                               parent = mode_stats,
                                               data = Stats(RAW_PROFILE_STATS, 0, 0))
        stat = stats.create_node(tag = time.time(),
                                 identifier = raw_stats_node.identifier + suite.name,
                                 parent = raw_stats_node,
                                 data = Stats(suite.name, pathsize(coverage_path), 0))
        raw_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, raw_stats_node.data.size)


        logger.info(f"{suite.name}: Converting raw profiles into indexed profiles - {stat.data}.")
        start_time = time.time()
        merge_result = await coverage_utils.merge_profiles(profiles = raw_profiles,
                                            path_for_merged = coverage_path,
                                            clear_on_success = (not options.coverage_keep_raw),
                                            semaphore = semaphore,
                                            logger = logger)
        indexed_stats_node = stats.get_node(mode_stats.identifier +INDEXED_PROFILE_STATS)
        if not indexed_stats_node:
            indexed_stats_node = stats.create_node(tag = time.time(),
                                                   identifier = mode_stats.identifier +INDEXED_PROFILE_STATS,
                                                   parent = mode_stats,
                                                   data = Stats(INDEXED_PROFILE_STATS, 0, 0))
        stat = stats.create_node(tag = time.time(),
                                 identifier = indexed_stats_node.identifier + suite.name,
                                 parent = indexed_stats_node,
                                 data = Stats(suite.name, pathsize(coverage_path), time.time() - start_time))
        indexed_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, indexed_stats_node.data.size)

        logger.info(f"{suite.name}: Done converting raw profiles into indexed profiles - {humanfriendly.format_timespan(stat.data.time)}.")

        # 2. Transform every indexed profile into an lcov trace file,
        #    after this step, the dependency upon the build artifacts
        #    ends and processing of the files can be done using the source
        #    code only.

        logger.info(f"{suite.name}: Converting indexed profiles into lcov trace files.")
        start_time = time.time()
        if len(merge_result.errors) > 0:
            raise RuntimeError(merge_result.errors)
        await coverage_utils.profdata_to_lcov(profiles = merge_result.generated_profiles,
                                              excludes = sources_to_exclude,
                                              known_file_ids = files_to_ids_map,
                                              clear_on_success = (not options.coverage_keep_indexed),
                                              semaphore = semaphore,
                                              logger = logger
                                              )
        lcov_conversion_stats_node = stats.get_node(mode_stats.identifier + LCOV_CONVERSION_STATS)
        if not lcov_conversion_stats_node:
            lcov_conversion_stats_node = stats.create_node(tag = time.time(),
                                                           identifier = mode_stats.identifier + LCOV_CONVERSION_STATS,
                                                           parent = mode_stats,
                                                           data = Stats(LCOV_CONVERSION_STATS, 0, 0))
        stat = stats.create_node(tag = time.time(),
                                 identifier = lcov_conversion_stats_node.identifier + suite.name,
                                 parent = lcov_conversion_stats_node,
                                 data = Stats(suite.name, pathsize(coverage_path), time.time() - start_time))
        lcov_conversion_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, lcov_conversion_stats_node.data.size)

        logger.info(f"{suite.name}: Done converting indexed profiles into lcov trace files - {humanfriendly.format_timespan(stat.data.time)}.")

        # 3. combine all tracefiles
        logger.info(f"{suite.name} in mode {suite.mode}: Combinig lcov trace files.")
        start_time = time.time()
        trace_files = list(coverage_path.glob("**/*.info"))
        target_trace_file = coverage_path / (suite.name + ".info")
        if len(trace_files) == 0: # No coverage data, can skip
            logger.warning(f"{suite.name} in mode  {suite.mode}: No coverage tracefiles found")
        elif len(trace_files) == 1: # No need to merge, we can just rename the file
            trace_files[0].rename(str(target_trace_file))
        else:
            await coverage_utils.lcov_combine_traces(lcovs = trace_files,
                                                     output_lcov = target_trace_file,
                                                     clear_on_success = (not options.coverage_keep_lcovs),
                                                     files_per_chunk = 10,
                                                     semaphore = semaphore,
                                                     logger = logger)
        lcov_merge_stats_node = stats.get_node(mode_stats.identifier + LCOV_SUITES_MEREGE_STATS)
        if not lcov_merge_stats_node:
            lcov_merge_stats_node = stats.create_node(tag = time.time(),
                                                      identifier = mode_stats.identifier + LCOV_SUITES_MEREGE_STATS,
                                                      parent = mode_stats,
                                                      data = Stats(LCOV_SUITES_MEREGE_STATS, 0, 0))
        stat = stats.create_node(tag = time.time(),
                                 identifier = lcov_merge_stats_node.identifier + suite.name,
                                 parent = lcov_merge_stats_node,
                                 data = Stats(suite.name, pathsize(coverage_path), time.time() - start_time))
        lcov_merge_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, lcov_merge_stats_node.data.size)

        suits_trace_files.setdefault(suite.mode, {})[suite.name] = target_trace_file
        logger.info(f"{suite.name}: Done combinig lcov trace files - {humanfriendly.format_timespan(stat.data.time)}")

    #4. combine the suite lcovs into per mode trace files
    modes_trace_files  = {}
    for mode, suite_traces in suits_trace_files.items():

        target_trace_file = pathlib.Path(options.tmpdir) / mode / "coverage" / f"{mode}_coverage.info"
        start_time = time.time()
        logger.info(f"Consolidating trace files for mode {mode}.")
        await coverage_utils.lcov_combine_traces(lcovs = suite_traces.values(),
                                                 output_lcov = target_trace_file,
                                                 clear_on_success = False,
                                                 files_per_chunk = 10,
                                                 semaphore = semaphore,
                                                 logger = logger)
        mode_stats = stats[mode]
        stat = stats.create_node(tag = time.time(),
                                 identifier = mode_stats.identifier + LCOV_MODES_MERGE_STATS,
                                 parent = mode_stats,
                                 data = Stats(LCOV_MODES_MERGE_STATS, None, time.time() - start_time))
        mode_stats.data.time += stat.data.time
        ROOT_NODE.data.size += mode_stats.data.size
        modes_trace_files[mode] = target_trace_file
        logger.info(f"Done consolidating trace files for mode {mode} - time: {humanfriendly.format_timespan(stat.data.time)}.")
    #5. create one consolidated file with all trace information
    logger.info(f"Consolidating all trace files for this run.")
    start_time = time.time()
    target_trace_file = pathlib.Path(options.tmpdir) / "test_coverage.info"
    await coverage_utils.lcov_combine_traces(lcovs = modes_trace_files.values(),
                                             output_lcov = target_trace_file,
                                             clear_on_success = False,
                                             files_per_chunk = 10,
                                             semaphore = semaphore,
                                             logger = logger)
    stats.create_node(tag = time.time(),
                      identifier = LCOV_MERGE_ALL_STATS,
                      parent = ROOT_NODE,
                      data = Stats(LCOV_MERGE_ALL_STATS, None, time.time() - start_time))
    logger.info(f"Done consolidating all trace files for this run - time: {humanfriendly.format_timespan(time.time() - start_time)}.")

    logger.info(f"Creating textual report.")
    proc = await asyncio.create_subprocess_shell(f"lcov --summary --rc lcov_branch_coverage=1 {options.tmpdir}/test_coverage.info 2>/dev/null > {options.tmpdir}/test_coverage_report.txt")
    await proc.wait()
    with open(pathlib.Path(options.tmpdir) /"test_coverage_report.txt") as f:
        summary = f.readlines()
    proc = await asyncio.create_subprocess_shell(f"lcov --list --rc lcov_branch_coverage=1 {options.tmpdir}/test_coverage.info  2>/dev/null >> {options.tmpdir}/test_coverage_report.txt")
    await proc.wait()
    logger.info(f"Done creating textual report. ({options.tmpdir}/test_coverage_report.txt)")
    total_processing_time = time.time() - total_processing_time
    ROOT_NODE.data.time = total_processing_time




    stats_str ="\n" + stats.show(stdout=False,
                                 data_property="asstring")
    summary = ["\n" + l for l in summary]
    logger.info(stats_str)
    logger.info("".join(summary))

async def workaround_python26789() -> int:
    """Workaround for https://bugs.python.org/issue26789.
    We'd like to print traceback if there is an internal error
    in test.py. However, traceback module calls asyncio
    default_exception_handler which in turns calls logging
    module after it has been shut down. This leads to a nested
    exception. Until 3.10 is in widespread use, reset the
    asyncio exception handler before printing traceback."""
    try:
        code = await main()
    except (Exception, KeyboardInterrupt):
        def noop(x, y):
            return None
        asyncio.get_running_loop().set_exception_handler(noop)
        traceback.print_exc()
        # Clear the custom handler
        asyncio.get_running_loop().set_exception_handler(None)
        return -1
    return code


if __name__ == "__main__":
    colorama.init()
    # gh-16583: ignore the inherited client host's ScyllaDB environment,
    # since it may break the tests
    if "SCYLLA_CONF" in os.environ:
        del os.environ["SCYLLA_CONF"]
    if "SCYLLA_HOME" in os.environ:
        del os.environ["SCYLLA_HOME"]

    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(workaround_python26789()))
