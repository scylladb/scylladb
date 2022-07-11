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
from test.pylib.scylla_server import ScyllaServer, ScyllaCluster
from typing import Dict, List, Callable, Any, Iterable, Optional, Awaitable

output_is_a_tty = sys.stdout.isatty()

all_modes = set(['debug', 'release', 'dev', 'sanitize', 'coverage'])
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


class TestSuite(ABC):
    """A test suite is a folder with tests of the same type.
    E.g. it can be unit tests, boost tests, or CQL tests."""

    # All existing test suites, one suite per path/mode.
    suites: Dict[str, 'TestSuite'] = dict()
    artifacts = ArtifactRegistry()
    hosts = HostRegistry()
    FLAKY_RETRIES = 5
    _next_id = 0

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

    @property
    def next_id(self) -> int:
        TestSuite._next_id += 1
        return TestSuite._next_id

    @staticmethod
    def test_count() -> int:
        return TestSuite._next_id

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
    async def add_test(self, shortname: str) -> None:
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
            if shortname in self.disabled_tests:
                continue

            t = os.path.join(self.name, shortname)
            patterns = options.name if options.name else [t]
            if options.skip_pattern and options.skip_pattern in t:
                continue

            async def add_test(shortname) -> None:
                # Add variants of the same test sequentially
                # so that case cache has a chance to populate
                for i in range(options.repeat):
                    await self.add_test(shortname)
                    self.pending_test_count += 1

            for p in patterns:
                if p in t:
                    pending.add(asyncio.create_task(add_test(shortname)))
        if len(pending) == 0:
            return
        try:
            await asyncio.gather(*pending)
        except asyncio.CancelledError:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise


class UnitTestSuite(TestSuite):
    """TestSuite instantiation for non-boost unit tests"""

    def __init__(self, path: str, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        # Map of custom test command line arguments, if configured
        self.custom_args = cfg.get("custom_args", {})

    async def create_test(self, shortname, suite, args):
        test = UnitTest(self.next_id, shortname, suite, args)
        self.tests.append(test)

    async def add_test(self, shortname) -> None:
        """Create a UnitTest class with possibly custom command line
        arguments and add it to the list of tests"""
        # Skip tests which are not configured, and hence are not built
        if os.path.join("test", self.name, shortname) not in self.options.tests:
            return

        # Default seastar arguments, if not provided in custom test options,
        # are two cores and 2G of RAM
        args = self.custom_args.get(shortname, ["-c2 -m2G"])
        for a in args:
            await self.create_test(shortname, self, a)

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

    async def create_test(self, shortname: str, suite, args) -> None:
        options = self.options
        if options.parallel_cases and (shortname not in self.no_parallel_cases):
            fqname = os.path.join(self.mode, self.name, shortname)
            if fqname not in self._case_cache:
                exe = os.path.join("build", suite.mode, "test", suite.name, shortname)
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
                test = BoostTest(self.next_id, shortname, suite, args, None)
                self.tests.append(test)
            else:
                for case in case_list:
                    test = BoostTest(self.next_id, shortname, suite, args, case)
                    self.tests.append(test)
        else:
            test = BoostTest(self.next_id, shortname, suite, args, None)
            self.tests.append(test)

    def junit_tests(self) -> Iterable['Test']:
        """Boost tests produce an own XML output, so are not included in a junit report"""
        return []


class PythonTestSuite(TestSuite):
    """A collection of Python pytests against a single Scylla instance"""

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = os.path.join("build", self.mode, "scylla")
        if self.mode == "coverage":
            self.scylla_env = coverage.env(self.scylla_exe, distinct_id=self.name)
        else:
            self.scylla_env = dict()
        self.scylla_env['SCYLLA'] = self.scylla_exe

        topology = self.cfg.get("topology", {"class": "simple", "replication_factor": 1})

        self.create_cluster = self.topology_for_class(topology["class"], topology)

        self.clusters = Pool(cfg.get("pool_size", 2), self.create_cluster)

    def topology_for_class(self, class_name: str, cfg: dict) -> Callable[[], Awaitable]:

        def create_server(cluster_name, seed):
            cmdline_options = self.cfg.get("extra_scylla_cmdline_options", [])
            if type(cmdline_options) == str:
                cmdline_options = [cmdline_options]
            server = ScyllaServer(
                exe=self.scylla_exe,
                vardir=os.path.join(self.options.tmpdir, self.mode),
                host_registry=self.hosts,
                cluster_name=cluster_name,
                seed=seed,
                cmdline_options=cmdline_options)

            # Suite artifacts are removed when
            # the entire suite ends successfully.
            self.artifacts.add_suite_artifact(self, server.stop_artifact)
            if not self.options.save_log_on_success:
                # If a test fails, we might want to keep the data dir.
                self.artifacts.add_suite_artifact(self, server.uninstall_artifact)
            self.artifacts.add_exit_artifact(self, server.stop_artifact)
            return server

        if class_name.lower() == "simple":
            async def create_cluster():
                cluster = ScyllaCluster(int(cfg["replication_factor"]),
                                        create_server)
                await cluster.install_and_start()
                return cluster

            return create_cluster
        else:
            raise RuntimeError("Unsupported topology name")

    def build_test_list(self) -> List[str]:
        """For pytest, search for directories recursively"""
        path = self.suite_path
        pytests = itertools.chain(path.rglob("*_test.py"), path.rglob("test_*.py"))
        return [os.path.splitext(t.relative_to(self.suite_path))[0] for t in pytests]

    @property
    def pattern(self) -> str:
        assert False

    async def add_test(self, shortname) -> None:
        test = PythonTest(self.next_id, shortname, self)
        self.tests.append(test)


class CQLApprovalTestSuite(PythonTestSuite):
    """Run CQL commands against a single Scylla instance"""

    def __init__(self, path, cfg, options: argparse.Namespace, mode) -> None:
        super().__init__(path, cfg, options, mode)

    async def add_test(self, shortname: str) -> None:
        test = CQLApprovalTest(self.next_id, shortname, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        return "*test.cql"


class RunTestSuite(TestSuite):
    """TestSuite for test directory with a 'run' script """

    def __init__(self, path: str, cfg, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = os.path.join("build", self.mode, "scylla")
        if self.mode == "coverage":
            self.scylla_env = coverage.env(self.scylla_exe, distinct_id=self.name)
        else:
            self.scylla_env = dict()
        self.scylla_env['SCYLLA'] = self.scylla_exe

    async def add_test(self, shortname) -> None:
        test = RunTest(self.next_id, shortname, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        return "run"


class Test:
    """Base class for CQL, Unit and Boost tests"""
    def __init__(self, test_no: int, shortname: str, suite) -> None:
        self.id = test_no
        self.path = ""
        self.args: List[str] = []
        # Name with test suite name
        self.name = os.path.join(suite.name, shortname.split('.')[0])
        # Name within the suite
        self.shortname = shortname
        self.mode = suite.mode
        self.suite = suite
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = "{}.{}".format(self.shortname, self.id)
        self.log_filename = pathlib.Path(suite.options.tmpdir) / self.mode / (self.uname + ".log")
        self.log_filename.parent.mkdir(parents=True, exist_ok=True)
        self.is_flaky = self.shortname in suite.flaky_tests
        # True if the test was retried after it failed
        self.is_flaky_failure = False
        # True if the test was cancelled by a ctrl-c or timeout, so
        # shouldn't be retried, even if it is flaky
        self.is_cancelled = False
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

    def get_junit_etree(self):
        return None

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
        self.path = os.path.join("build", self.mode, "test", self.name)
        self.args = shlex.split(args) + UnitTest.standard_args
        if self.mode == "coverage":
            self.env = coverage.env(self.path)
        else:
            self.env = dict()
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


class BoostTest(UnitTest):
    """A unit test which can produce its own XML output"""

    def __init__(self, test_no: int, shortname: str, suite, args: str,
                 casename: Optional[str]) -> None:
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
        self.__junit_etree: Optional[ET.ElementTree] = None

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.__junit_etree = None

    def get_junit_etree(self) -> ET.ElementTree:
        def adjust_suite_name(name):
            # Normalize "path/to/file.cc" to "path.to.file" to conform to
            # Jenkins expectations that the suite name is a class name. ".cc"
            # doesn't add any infomation. Add the mode, otherwise failures
            # in different modes are indistinguishable. The "test/" prefix adds
            # no information, so remove it.
            import re
            name = re.sub(r'^test/', '', name)
            name = re.sub(r'\.cc$', '', name)
            name = re.sub(r'/', '.', name)
            name = f'{name}.{self.mode}'
            return name
        if self.__junit_etree is None:
            self.__junit_etree = ET.parse(self.xmlout)
            root = self.__junit_etree.getroot()
            suites = root.findall('.//TestSuite')
            for suite in suites:
                suite.attrib['name'] = adjust_suite_name(suite.attrib['name'])
                skipped = suite.findall('./TestCase[@reason="disabled"]')
                for e in skipped:
                    suite.remove(e)
            os.unlink(self.xmlout)
        return self.__junit_etree

    def check_log(self, trim: bool) -> None:
        self.get_junit_etree()
        super().check_log(trim)

    async def run(self, options):
        if options.random_seed:
            self.args += ['--random-seed', options.random_seed]
        return await super().run(options)

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        """Does not write junit report for Jenkins legacy reasons"""
        assert False


class CQLApprovalTest(Test):
    """Run a sequence of CQL commands against a standlone Scylla"""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        # Path to cql_repl driver, in the given build mode
        self.path = "pytest"
        self.cql = suite.suite_path / (self.shortname + ".cql")
        self.result = suite.suite_path / (self.shortname + ".result")
        self.tmpfile = os.path.join(suite.options.tmpdir, self.mode, self.uname + ".reject")
        self.reject = suite.suite_path / (self.shortname + ".reject")
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
        self.env: Dict[str, str] = dict()
        old_tmpfile = pathlib.Path(self.tmpfile)
        if old_tmpfile.exists():
            old_tmpfile.unlink()
        self.args = [
            "-s",  # don't capture print() inside pytest
            "test/pylib/cql_repl/cql_repl.py",
            "--input={}".format(self.cql),
            "--output={}".format(self.tmpfile),
        ]

    async def run(self, options: argparse.Namespace) -> Test:
        self.success = False
        self.summary = "failed"

        def set_summary(summary):
            self.summary = summary
            logging.info("Test %s %s", self.uname, summary)
            if self.server_log:
                logging.info("Server log:\n%s", self.server_log)

        async with self.suite.clusters.instance() as cluster:
            logging.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
            self.args.insert(1, "--host={}".format(cluster[0].host))
            # If pre-check fails, e.g. because Scylla failed to start
            # or crashed between two tests, fail entire test.py
            try:
                cluster.before_test(self.uname)
                self.is_before_test_ok = True
                cluster[0].take_log_savepoint()
                self.is_executed_ok = await run_test(self, options, env=self.env)
                cluster.after_test(self.uname)
                self.is_after_test_ok = True

                if self.is_executed_ok is False:
                    set_summary("""returned non-zero return status.\n
Check test log at {}.""".format(self.log_filename))
                elif not os.path.isfile(self.tmpfile):
                    set_summary("failed: no output file")
                elif not os.path.isfile(self.result):
                    set_summary("failed: no result file")
                    self.is_new = True
                else:
                    self.is_equal_result = filecmp.cmp(self.result, self.tmpfile)
                    if self.is_equal_result is False:
                        self.unidiff = format_unidiff(self.result, self.tmpfile)
                        set_summary("failed: test output does not match expected result")
                        assert self.unidiff is not None
                        logging.info("\n{}".format(palette.nocolor(self.unidiff)))
                    else:
                        self.success = True
                        set_summary("succeeded")
            except Exception as e:
                # Server log bloats the output if we produce it in all
                # cases. So only grab it when it's relevant:
                # 1) failed pre-check, e.g. start failure
                # 2) failed test execution.
                if self.is_executed_ok is False:
                    self.server_log = cluster[0].read_log()
                    if self.is_before_test_ok is False:
                        set_summary("pre-check failed: {}".format(e))
                        print("Test {} {}".format(self.name, self.summary))
                        print("Server log  of the first server:\n{}".format(self.server_log))
                        # Don't try to continue if the cluster is broken
                        raise
                set_summary("failed: {}".format(e))
            finally:
                if self.is_new or self.is_equal_result is False:
                    # Put a copy of the .reject file close to the .result file
                    # so that it's easy to analyze the diff or overwrite .result
                    # with .reject. Preserve the original .reject file: in
                    # multiple modes the copy .reject file may be overwritten.
                    shutil.copyfile(self.tmpfile, self.reject)
                elif os.path.exists(self.tmpfile):
                    pathlib.Path(self.tmpfile).unlink()

        return self

    def print_summary(self) -> None:
        print("Test {} ({}) {}".format(palette.path(self.name), self.mode,
                                       self.summary))
        if self.is_executed_ok is False:
            print(read_log(self.log_filename))
            if self.server_log:
                print("Server log of the first server:")
                print(self.server_log)
        elif self.is_equal_result is False and self.unidiff:
            print(self.unidiff)

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        assert not self.success
        xml_fail = ET.SubElement(xml_res, 'failure')
        xml_fail.text = self.summary
        if self.is_executed_ok is False:
            if self.log_filename.exists():
                system_out = ET.SubElement(xml_res, 'system-out')
                system_out.text = read_log(self.log_filename)
            if self.server_log:
                system_err = ET.SubElement(xml_res, 'system-err')
                system_err.text = read_log(self.server_log)
        elif self.unidiff:
            system_out = ET.SubElement(xml_res, 'system-out')
            system_out.text = palette.nocolor(self.unidiff)


class RunTest(Test):
    """Run tests in a directory started by a run script"""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = suite.suite_path / shortname
        self.xmlout = os.path.join(suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.args = ["--junit-xml={}".format(self.xmlout)]
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

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = "pytest"
        self.xmlout = os.path.join(self.suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        PythonTest._reset(self)

    def _reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.server_log = None
        self.is_before_test_ok = False
        self.is_after_test_ok = False
        self.args = [
            "-s",  # don't capture print() output inside pytest
            "-o",
            "junit_family=xunit2",
            "--junit-xml={}".format(self.xmlout),
            str(self.suite.suite_path / (self.shortname + ".py"))]

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))
        if self.server_log:
            print("Server log of the first server:")
            print(self.server_log)

    async def run(self, options: argparse.Namespace) -> Test:

        async with self.suite.clusters.instance() as cluster:
            logging.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
            self.args.insert(0, "--host={}".format(cluster[0].host))
            try:
                cluster.before_test(self.uname)
                self.is_before_test_ok = True
                cluster[0].take_log_savepoint()
                status = await run_test(self, options)
                cluster.after_test(self.uname)
                self.is_after_test_ok = True
                self.success = status
            except Exception as e:
                self.server_log = cluster[0].read_log()
                if self.is_before_test_ok is False:
                    print("Test {} pre-check failed: {}".format(self.name, str(e)))
                    print("Server log of the first server:\n{}".format(self.server_log))
                    # Don't try to continue if the cluster is broken
                    raise
            logging.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        super().write_junit_failure_report(xml_res)
        if self.server_log:
            system_err = ET.SubElement(xml_res, 'system-err')
            system_err.text = read_log(self.server_log)


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
        if self.verbose is False:
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
                         **env,
                         ),
                preexec_fn=os.setsid,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), options.timeout)
            test.time_end = time.time()
            if process.returncode != 0:
                report_error('Test exited with code {code}\n'.format(code=process.returncode))
                return False
            try:
                test.check_log(not options.save_log_on_success)
            except Exception as e:
                print("")
                print(test.name + ": " + palette.crit("failed to parse XML output: {}".format(e)))
                # return False
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
                tests starting with "mem" in "boost" suite. Default: run all
                tests in all suites.""",
    )
    parser.add_argument(
        "--tmpdir",
        action="store",
        default="testlog",
        help="""Path to temporary test data and log files. The data is
        further segregated per build mode. Default: ./testlog.""",
    )
    parser.add_argument('--mode', choices=all_modes, action="append", dest="modes",
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
    parser.add_argument('--skip', default="",
                        dest="skip_pattern", action="store",
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
            out = subprocess.Popen(['ninja', 'mode_list'], stdout=subprocess.PIPE).communicate()[0].decode()
            # [1/1] List configured modes
            # debug release dev
            args.modes = re.sub(r'.* List configured modes\n(.*)\n', r'\1',
                                out, 1, re.DOTALL).split("\n")[-1].split(' ')
        except Exception:
            print(palette.fail("Failed to read output of `ninja mode_list`: please run ./configure.py first"))
            raise

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

    # Get the list of tests configured by configure.py
    try:
        out = subprocess.Popen(['ninja', 'unit_test_list'], stdout=subprocess.PIPE).communicate()[0].decode()
        # [1/1] List configured unit tests
        args.tests = set(re.sub(r'.* List configured unit tests\n(.*)\n', r'\1', out, 1, re.DOTALL).split("\n"))
    except Exception:
        print(palette.fail("Failed to read output of `ninja unit_test_list`: please run ./configure.py first"))
        raise

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
            xml_res = ET.SubElement(xml_results, 'testcase',
                                    name="{}.{}.{}".format(test.shortname, mode, test.id))
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


def write_consolidated_boost_junit_xml(tmpdir: str, mode: str) -> None:
    xml = ET.Element("TestLog")
    for suite in TestSuite.suites.values():
        for test in suite.tests:
            if test.mode != mode:
                continue
            test_xml = test.get_junit_etree()
            if test_xml is not None:
                xml.extend(test_xml.getroot().findall('.//TestSuite'))
    et = ET.ElementTree(xml)
    et.write(f'{tmpdir}/{mode}/xml/boost.xunit.xml', encoding='unicode')


def open_log(tmpdir: str, log_level: str) -> None:
    pathlib.Path(tmpdir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(tmpdir, "test.py.log"),
        filemode="w",
        level=log_level,
        format="%(asctime)s.%(msecs)03d %(levelname)s> %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.critical("Started %s", " ".join(sys.argv))


async def main() -> int:

    options = parse_cmd_line()

    open_log(options.tmpdir, options.log_level)

    await find_tests(options)
    if options.list_tests:
        print('\n'.join([t.name for t in TestSuite.all_tests()]))
        return 0

    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_event_loop(), signaled)

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
        write_junit_report(options.tmpdir, mode)
        write_consolidated_boost_junit_xml(options.tmpdir, mode)

    if 'coverage' in options.modes:
        coverage.generate_coverage_report("build/coverage", "tests")

    # Note: failure codes must be in the ranges 0-124, 126-127,
    #       to cooperate with git bisect's expectations
    return 0 if not failed_tests else 1


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
        asyncio.get_event_loop().set_exception_handler(noop)
        traceback.print_exc()
        # Clear the custom handler
        asyncio.get_event_loop().set_exception_handler(None)
        return -1
    return code


if __name__ == "__main__":
    colorama.init()

    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(workaround_python26789()))
