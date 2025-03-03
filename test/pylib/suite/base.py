#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import asyncio
import argparse
import collections
import itertools
import logging
import os
import pathlib
import re
import shlex
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from importlib import import_module
from typing import TYPE_CHECKING

import colorama
import yaml

from test.pylib.artifact_registry import ArtifactRegistry
from test.pylib.host_registry import HostRegistry
from test.pylib.resource_gather import get_resource_gather

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from typing import Any, Dict, List


all_modes = {'debug': 'Debug',
             'release': 'RelWithDebInfo',
             'dev': 'Dev',
             'sanitize': 'Sanitize',
             'coverage': 'Coverage'}
debug_modes = {'debug', 'sanitize'}


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


def path_to(mode, *components):
    """Resolve path to built executable"""
    build_dir = 'build'
    if os.path.exists(os.path.join(build_dir, 'build.ninja')):
        *dir_components, basename = components
        return os.path.join(build_dir, *dir_components, all_modes[mode], basename)
    return os.path.join(build_dir, mode, *components)


class TestSuite(ABC):
    """A test suite is a folder with tests of the same type.
    E.g. it can be unit tests, boost tests, or CQL tests."""

    # All existing test suites, one suite per path/mode.
    suites: Dict[str, 'TestSuite'] = dict()
    artifacts: ArtifactRegistry
    hosts: HostRegistry
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

            SpecificTestSuite = getattr(import_module("test.pylib.suite"), suite_type_to_class_name(kind), None)
            if not SpecificTestSuite:
                raise RuntimeError("Failed to load tests in {}: suite type '{}' not found".format(path, kind))
            suite = SpecificTestSuite(path, cfg, options, mode)
            assert suite is not None
            TestSuite.suites[suite_key] = suite
        return suite

    @staticmethod
    def all_tests() -> Iterable['Test']:
        return itertools.chain(*(suite.tests for suite in
                                 TestSuite.suites.values()))

    @property
    @abstractmethod
    def pattern(self) -> str:
        pass

    @abstractmethod
    async def add_test(self, shortname: str, casename: str) -> None:
        pass

    async def run(self, test: 'Test', options: argparse.Namespace):
        try:
            test.started = True
            for i in range(1, self.FLAKY_RETRIES):
                if i > 1:
                    test.is_flaky_failure = True
                    logging.info("Retrying test %s after a flaky fail, retry %d", test.uname, i)
                    test.reset()
                await test.run(options)
                if test.success or not test.is_flaky or test.is_cancelled:
                    break
        except asyncio.CancelledError:
            test.is_cancelled = True
            raise
        finally:
            self.pending_test_count -= 1
            self.n_failed += int(test.failed)
            if self.pending_test_count == 0:
                await TestSuite.artifacts.cleanup_after_suite(self, self.n_failed > 0)
        return test

    def junit_tests(self):
        """Tests which participate in a consolidated junit report"""
        return self.tests

    def boost_tests(self):
        return []

    def build_test_list(self) -> List[str]:
        pattern = self.pattern if isinstance(self.pattern, list) else [self.pattern]
        tests = itertools.chain(*[self.suite_path.rglob(i) for i in pattern])
        return [os.path.splitext(t.relative_to(self.suite_path))[0] for t in tests]

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


class Test:
    """Base class for CQL, Unit and Boost tests"""
    def __init__(self, test_no: int, shortname: str, suite) -> None:
        self.id = test_no
        self.path = ""
        self.args: List[str] = []
        # Arguments which are required by a program regardless of additional test specific arguments
        self.core_args : List[str] = []
        self.valid_exit_codes = [0]
        # Name with test suite name
        self.name = os.path.join(suite.name, shortname.split('.')[0])
        # Name within the suite
        self.shortname = shortname
        self.mode = suite.mode
        self.suite = suite
        self.allure_dir = pathlib.Path(suite.options.tmpdir) / self.mode / 'allure'
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = "{}.{}.{}".format(self.suite.name, self.shortname.replace("/","."), self.id)
        self.log_filename = pathlib.Path(suite.options.tmpdir) / self.mode / (self.uname + ".log")
        self.log_filename.parent.mkdir(parents=True, exist_ok=True)
        self.is_flaky = self.shortname in suite.flaky_tests
        # True if the test was retried after it failed
        self.is_flaky_failure = False
        # True if the test was cancelled by a ctrl-c or timeout, so
        # shouldn't be retried, even if it is flaky
        self.is_cancelled = False
        self.env = dict(self.suite.base_env)
        self.started = False
        self.success = False
        self.time_start: float = 0
        self.time_end: float = 0

    def reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        self.success = False
        self.time_start = 0
        self.time_end = 0

    @property
    def failed(self):
        """Returns True, if this test Failed"""
        return self.started and not self.success and not self.is_cancelled

    @property
    def did_not_run(self):
        """Returns True, if this test did not run correctly, i.e. was canceled either during or before execution"""
        return not self.started or self.is_cancelled

    @abstractmethod
    async def run(self, options: argparse.Namespace) -> 'Test':
        pass

    @abstractmethod
    def print_summary(self) -> None:
        pass

    async def setup(self, port, options):
        """
        Performs any necessary setup steps before running a test.
        Returns (fn, txt, test_env) where:
        fn  - is a cleanup function to call unconditionally after the test stops running
        txt - is failure-injection description.
        test_env - is a dictionary containing environment variables map specific for the test
        """
        return (lambda: 0, None,{})

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


def init_testsuite_globals() -> None:
    """Create global objects required for a test run."""

    TestSuite.artifacts = ArtifactRegistry()
    TestSuite.hosts = HostRegistry()


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


toxiproxy_id_gen = 0


async def run_test(test: Test, options: argparse.Namespace, gentle_kill=False, env=dict()) -> bool:
    """Run test program, return True if success else False"""

    with test.log_filename.open("wb") as log:
        global toxiproxy_id_gen
        toxiproxy_id = toxiproxy_id_gen
        toxiproxy_id_gen += 1
        ldap_port = 5000 + (toxiproxy_id * 3) % 55000
        cleanup_fn = None
        finject_desc = None
        def report_error(error, failure_injection_desc = None):
            msg = "=== TEST.PY SUMMARY START ===\n"
            msg += "{}\n".format(error)
            msg += "=== TEST.PY SUMMARY END ===\n"
            if failure_injection_desc is not None:
                msg += 'failure injection: {}'.format(failure_injection_desc)
            log.write(msg.encode(encoding="UTF-8"))

        try:
            cleanup_fn, finject_desc, test_env = await test.setup(ldap_port, options)
        except Exception as e:
            report_error("Test setup failed ({})\n{}".format(str(e), traceback.format_exc()))
            return False
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
        ldap_instance_path = os.path.join(
            os.path.abspath(os.path.join(options.tmpdir, test.mode, 'ldap_instances')),
            str(ldap_port))
        saslauthd_mux_path = os.path.join(ldap_instance_path, 'mux')
        if options.manual_execution:
            print('Please run the following shell command, then press <enter>:')
            test_env_string = " ".join([f"{k}={v}" for k,v in test_env.items()])
            print('{} {}'.format(
                test_env_string, ' '.join([shlex.quote(e) for e in [test.path, *test.args]])))
            input('-- press <enter> to continue --')
            if cleanup_fn is not None:
                cleanup_fn()
            return True
        try:
            resource_gather = get_resource_gather(options.gather_metrics, test, options.tmpdir)
            resource_gather.make_cgroup()
            log.write("=== TEST.PY STARTING TEST {} ===\n".format(test.uname).encode(encoding="UTF-8"))
            log.write("export UBSAN_OPTIONS='{}'\n".format(
                ":".join(filter(None, UBSAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("export ASAN_OPTIONS='{}'\n".format(
                ":".join(filter(None, ASAN_OPTIONS))).encode(encoding="UTF-8"))
            for k,v in test_env.items():
                log.write(f"export {k}={v}\n".encode(encoding="UTF-8"))
            log.write("{} {}\n".format(test.path, " ".join(test.args)).encode(encoding="UTF-8"))
            log.write("=== TEST.PY TEST {} OUTPUT ===\n".format(test.uname).encode(encoding="UTF-8"))
            log.flush()
            test.time_start = time.time()
            test.time_end = 0

            path = test.path
            args = test.core_args + test.args
            if options.cpus:
                path = 'taskset'
                args = ['-c', options.cpus, test.path, *args]

            test_running_event = asyncio.Event()
            test_resource_watcher = resource_gather.cgroup_monitor(test_event=test_running_event)

            test_env.update(
                dict(os.environ,
                     UBSAN_OPTIONS=":".join(filter(None, UBSAN_OPTIONS)),
                     ASAN_OPTIONS=":".join(filter(None, ASAN_OPTIONS)),
                     # TMPDIR env variable is used by any seastar/scylla
                     # test for directory to store test temporary data.
                     TMPDIR=os.path.join(options.tmpdir, test.mode),
                     SCYLLA_TEST_ENV='yes',
                     SCYLLA_TEST_RUNNER="test.py",
                     **env,
                     )
            )
            process = await asyncio.create_subprocess_exec(
                path, *args,
                stderr=log,
                stdout=log,
                env=test_env,
                preexec_fn=resource_gather.put_process_to_cgroup,
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), options.timeout)
            test_running_event.set()
            test.time_end = time.time()

            metrics = resource_gather.get_test_metrics()
            try:
                async with asyncio.timeout(2):
                    await test_resource_watcher
            except TimeoutError:
                log.write(f'Metrics for {test.name} can be inaccurate, job reached timeout'.encode(encoding='UTF-8'))
            finally:
                resource_gather.remove_cgroup()

            if process.returncode not in test.valid_exit_codes:
                report_error('Test exited with code {code}\n'.format(code=process.returncode))
                resource_gather.write_metrics_to_db(metrics)
                return False
            try:
                test.check_log(not options.save_log_on_success)
            except Exception as e:
                print("")
                print(test.name + ": " + palette.crit("failed to parse XML output: {}".format(e)))
                resource_gather.write_metrics_to_db(metrics)
                return False
            resource_gather.write_metrics_to_db(metrics, True)
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
        finally:
            if cleanup_fn is not None:
                cleanup_fn()
    return False
