#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-present ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#
from abc import ABC, abstractmethod
import argparse
import asyncio
import colorama
import difflib
import filecmp
import glob
import io
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
import xml.etree.ElementTree as ET
import yaml

from scripts import coverage

output_is_a_tty = sys.stdout.isatty()

all_modes = set(['debug', 'release', 'dev', 'sanitize', 'coverage'])
debug_modes = set(['debug', 'sanitize'])


def create_formatter(*decorators):
    """Return a function which decorates its argument with the given
    color/style if stdout is a tty, and leaves intact otherwise."""
    def color(arg):
        return "".join(decorators) + str(arg) + colorama.Style.RESET_ALL

    def nocolor(arg):
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


class TestSuite(ABC):
    """A test suite is a folder with tests of the same type.
    E.g. it can be unit tests, boost tests, or CQL tests."""

    # All existing test suites, one suite per path.
    suites = dict()
    _next_id = 0

    def __init__(self, path, cfg):
        self.path = path
        self.name = os.path.basename(self.path)
        self.cfg = cfg
        self.tests = []

        self.run_first_tests = set(cfg.get("run_first", []))
        disabled = self.cfg.get("disable", [])
        non_debug = self.cfg.get("skip_in_debug_modes", [])
        self.enabled_modes = dict()
        self.disabled_tests = dict()
        for mode in all_modes:
            self.disabled_tests[mode] = \
                set(self.cfg.get("skip_in_" + mode, []) + (non_debug if mode in debug_modes else []) + disabled)
            for shortname in set(self.cfg.get("run_in_" + mode, [])):
                self.enabled_modes[shortname] = self.enabled_modes.get(shortname, []) + [mode]

    @property
    def next_id(self):
        TestSuite._next_id += 1
        return TestSuite._next_id

    @staticmethod
    def test_count():
        return TestSuite._next_id

    @staticmethod
    def load_cfg(path):
        with open(os.path.join(path, "suite.yaml"), "r") as cfg_file:
            cfg = yaml.safe_load(cfg_file.read())
            if not isinstance(cfg, dict):
                raise RuntimeError("Failed to load tests in {}: suite.yaml is empty".format(path))
            return cfg

    @staticmethod
    def opt_create(path):
        """Return a subclass of TestSuite with name cfg["type"].title + TestSuite.
        Ensures there is only one suite instance per path."""
        suite = TestSuite.suites.get(path)
        if not suite:
            cfg = TestSuite.load_cfg(path)
            kind = cfg.get("type")
            if kind is None:
                raise RuntimeError("Failed to load tests in {}: suite.yaml has no suite type".format(path))
            SpecificTestSuite = globals().get(kind.title() + "TestSuite")
            if not SpecificTestSuite:
                raise RuntimeError("Failed to load tests in {}: suite type '{}' not found".format(path, kind))
            suite = SpecificTestSuite(path, cfg)
            TestSuite.suites[path] = suite
        return suite

    @staticmethod
    def tests():
        return itertools.chain(*[suite.tests for suite in
                                 TestSuite.suites.values()])

    @property
    @abstractmethod
    def pattern(self):
        pass

    @abstractmethod
    def add_test(self, name, args, mode, options):
        pass

    def junit_tests(self):
        """Tests which participate in a consolidated junit report"""
        return self.tests

    def add_test_list(self, mode, options):
        lst = [ os.path.splitext(os.path.basename(t))[0] for t in glob.glob(os.path.join(self.path, self.pattern)) ]
        if lst:
            # Some tests are long and are better to be started earlier,
            # so pop them up while sorting the list
            lst.sort(key=lambda x: (x not in self.run_first_tests, x))

        for shortname in lst:
            if shortname in self.disabled_tests[mode]:
                continue
            enabled_modes = self.enabled_modes.get(shortname, [])
            if enabled_modes and mode not in enabled_modes:
                continue

            t = os.path.join(self.name, shortname)
            patterns = options.name if options.name else [t]
            if options.skip_pattern and options.skip_pattern in t:
                continue

            for p in patterns:
                if p in t:
                    for i in range(options.repeat):
                        self.add_test(shortname, mode, options)


class UnitTestSuite(TestSuite):
    """TestSuite instantiation for non-boost unit tests"""

    def __init__(self, path, cfg):
        super().__init__(path, cfg)
        # Map of custom test command line arguments, if configured
        self.custom_args = cfg.get("custom_args", {})

    def create_test(self, *args, **kwargs):
        return UnitTest(*args, **kwargs)

    def add_test(self, shortname, mode, options):
        """Create a UnitTest class with possibly custom command line
        arguments and add it to the list of tests"""
        # Skip tests which are not configured, and hence are not built
        if os.path.join("test", self.name, shortname) not in options.tests:
            return

        # Default seastar arguments, if not provided in custom test options,
        # are two cores and 2G of RAM
        args = self.custom_args.get(shortname, ["-c2 -m2G"])
        for a in args:
            test = self.create_test(self.next_id, shortname, a, self, mode, options)
            self.tests.append(test)

    @property
    def pattern(self):
        return "*_test.cc"


class BoostTestSuite(UnitTestSuite):
    """TestSuite for boost unit tests"""

    def create_test(self, *args, **kwargs):
        return BoostTest(*args, **kwargs)

    def junit_tests(self):
        """Boost tests produce an own XML output, so are not included in a junit report"""
        return []


class CqlTestSuite(TestSuite):
    """TestSuite for CQL tests"""

    def add_test(self, shortname, mode, options):
        """Create a CqlTest class and add it to the list"""
        test = CqlTest(self.next_id, shortname, self, mode, options)
        self.tests.append(test)

    @property
    def pattern(self):
        return "*_test.cql"

class RunTestSuite(TestSuite):
    """TestSuite for test directory with a 'run' script """

    def add_test(self, shortname, mode, options):
        test = RunTest(self.next_id, shortname, self, mode, options)
        self.tests.append(test)

    @property
    def pattern(self):
        return "run"


class Test:
    """Base class for CQL, Unit and Boost tests"""
    def __init__(self, test_no, shortname, suite, mode, options):
        self.id = test_no
        # Name with test suite name
        self.name = os.path.join(suite.name, shortname)
        # Name within the suite
        self.shortname = shortname
        self.mode = mode
        self.suite = suite
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = "{}.{}".format(self.shortname, self.id)
        self.log_filename = os.path.join(options.tmpdir, self.mode, self.uname + ".log")
        self.success = None

    @abstractmethod
    async def run(self, options):
        pass

    @abstractmethod
    def print_summary(self):
        pass


    def check_log(self, trim):
        """Check and trim logs and xml output for tests which have it"""
        if trim:
            pathlib.Path(self.log_filename).unlink()
        pass


class UnitTest(Test):
    standard_args = shlex.split("--overprovisioned --unsafe-bypass-fsync 1 --kernel-page-cache 1 --blocked-reactor-notify-ms 2000000 --collectd 0")

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        self.path = os.path.join("build", self.mode, "test", self.name)
        self.args = shlex.split(args) + UnitTest.standard_args
        if self.mode == "coverage":
            self.env = coverage.env(self.path)
        else:
            self.env = dict()

    def print_summary(self):
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options):
        self.success = await run_test(self, options, env=self.env)
        logging.info("Test #%d %s", self.id, "succeeded" if self.success else "failed ")
        return self


class BoostTest(UnitTest):
    """A unit test which can produce its own XML output"""

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, args, suite, mode, options)
        boost_args = []
        self.xmlout = os.path.join(options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        boost_args += ['--report_level=no',
                       '--logger=HRF,test_suite:XML,test_suite,' + self.xmlout]
        boost_args += ['--catch_system_errors=no']  # causes undebuggable cores
        boost_args += ['--color_output={}'.format('true' if output_is_a_tty else 'false')]
        boost_args += ['--']
        self.args = boost_args + self.args

    def check_log(self, trim):
        ET.parse(self.xmlout)
        super().check_log(trim)


class CqlTest(Test):
    """Run the sequence of CQL commands stored in the file and check
    output"""

    def __init__(self, test_no, shortname, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        # Path to cql_repl driver, in the given build mode
        self.path = os.path.join("build", self.mode, "test/tools/cql_repl")
        self.cql = os.path.join(suite.path, self.shortname + ".cql")
        self.result = os.path.join(suite.path, self.shortname + ".result")
        self.tmpfile = os.path.join(options.tmpdir, self.mode, self.uname + ".reject")
        self.reject = os.path.join(suite.path, self.shortname + ".reject")
        self.args = shlex.split("-c1 -m2G --input={} --output={} --log={}".format(
            self.cql, self.tmpfile, self.log_filename))
        self.args += UnitTest.standard_args
        self.is_executed_ok = False
        self.is_new = False
        self.is_equal_result = None
        self.summary = "not run"
        if self.mode == "coverage":
            self.env = coverage.env(self.path, distinct_id=self.id)
        else:
            self.env = dict()

    async def run(self, options):
        self.is_executed_ok = await run_test(self, options, env=self.env)

        self.success = False
        self.summary = "failed"

        def set_summary(summary):
            self.summary = summary
            logging.info("Test %d %s", self.id, summary)

        if not os.path.isfile(self.tmpfile):
            set_summary("failed: no output file")
        elif not os.path.isfile(self.result):
            set_summary("failed: no result file")
            self.is_new = True
        else:
            self.is_equal_result = filecmp.cmp(self.result, self.tmpfile)
            if self.is_equal_result is False:
                set_summary("failed: test output does not match expected result")
            elif self.is_executed_ok:
                self.success = True
                set_summary("succeeded")
            else:
                set_summary("failed: correct output but non-zero return status.\nCheck test log.")

        if self.is_new or self.is_equal_result is False:
            # Put a copy of the .reject file close to the .result file
            # so that it's easy to analyze the diff or overwrite .result
            # with .reject. Preserve the original .reject file: in
            # multiple modes the copy .reject file may be overwritten.
            shutil.copyfile(self.tmpfile, self.reject)
        elif os.path.exists(self.tmpfile):
            pathlib.Path(self.tmpfile).unlink()

        return self

    def print_summary(self):
        print("Test {} ({}) {}".format(palette.path(self.name), self.mode,
                                       self.summary))
        if self.is_equal_result is False:
            print_unidiff(self.result, self.reject)

class RunTest(Test):
    """Run tests in a directory started by a run script"""

    def __init__(self, test_no, shortname, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        self.path = os.path.join(suite.path, shortname)
        self.xmlout = os.path.join(options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.args = ["--junit-xml={}".format(self.xmlout)]
        self.scylla_path = os.path.join("build", self.mode, "scylla")

        if self.mode == "coverage":
            self.env = coverage.env(self.scylla_path, distinct_id=self.suite.name)
        else:
            self.env = dict()
        self.env['SCYLLA'] = self.scylla_path

    def print_summary(self):
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options):
        # This test can and should be killed gently, with SIGTERM, not with SIGKILL
        self.success = await run_test(self, options, gentle_kill=True, env=self.env)
        logging.info("Test #%d %s", self.id, "succeeded" if self.success else "failed ")
        return self

class TabularConsoleOutput:
    """Print test progress to the console"""

    def __init__(self, verbose, test_count):
        self.verbose = verbose
        self.test_count = test_count
        self.print_newline = False
        self.last_test_no = 0
        self.last_line_len = 1

    def print_start_blurb(self):
        print("="*80)
        print("{:7s} {:50s} {:^8s} {:8s}".format("[N/TOTAL]", "TEST", "MODE", "RESULT"))
        print("-"*78)

    def print_end_blurb(self):
        if self.print_newline:
            print("")
        print("-"*78)

    def print_progress(self, test):
        self.last_test_no += 1
        msg = "{:9s} {:50s} {:^8s} {:8s}".format(
            "[{}/{}]".format(self.last_test_no, self.test_count),
            test.name, test.mode[:8],
            palette.ok("[ PASS ]") if test.success else palette.fail("[ FAIL ]")
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
            if hasattr(test, 'time_end') and test.time_end > 0:
                msg += " {:.2f}s".format(test.time_end - test.time_start)
            print(msg)


async def run_test(test, options, gentle_kill=False, env=dict()):
    """Run test program, return True if success else False"""

    with open(test.log_filename, "wb") as log:

        def report_error(error):
            msg = "=== TEST.PY SUMMARY START ===\n"
            msg += "{}\n".format(error)
            msg += "=== TEST.PY SUMMARY END ===\n"
            log.write(msg.encode(encoding="UTF-8"))
        process = None
        stdout = None
        logging.info("Starting test #%d: %s %s", test.id, test.path, " ".join(test.args))
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
            log.write("=== TEST.PY STARTING TEST #{} ===\n".format(test.id).encode(encoding="UTF-8"))
            log.write("export UBSAN_OPTIONS='{}'\n".format(":".join(filter(None, UBSAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("export ASAN_OPTIONS='{}'\n".format(":".join(filter(None, ASAN_OPTIONS))).encode(encoding="UTF-8"))
            log.write("{} {}\n".format(test.path, " ".join(test.args)).encode(encoding="UTF-8"))
            log.write("=== TEST.PY TEST OUTPUT ===\n".format(test.id).encode(encoding="UTF-8"))
            log.flush();
            test.time_start = time.time()
            test.time_end = 0
            process = await asyncio.create_subprocess_exec(
                test.path,
                *test.args,
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
            if process is not None:
                if gentle_kill:
                    process.terminate()
                else:
                    process.kill()
                stdout, _ = await process.communicate()
            if isinstance(e, asyncio.TimeoutError):
                report_error("Test timed out")
            elif isinstance(e, asyncio.CancelledError):
                print(test.name, end=" ")
                report_error("Test was cancelled")
        except Exception as e:
            report_error("Failed to run the test:\n{e}".format(e=e))
    return False


def setup_signal_handlers(loop, signaled):

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


def parse_cmd_line():
    """ Print usage and process command line options. """
    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 6e9 if os.sysconf('SC_PAGE_SIZE') > 4096 else 2e9
    cpus_per_test_job = 1
    default_num_jobs_mem = ((sysmem - 4e9) // testmem)
    default_num_jobs_cpu = multiprocessing.cpu_count() // cpus_per_test_job
    default_num_jobs = min(default_num_jobs_mem, default_num_jobs_cpu)

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
    parser.add_argument('--timeout', action="store", default="12000", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument('--save-log-on-success', "-s", default=False,
                        dest="save_log_on_success", action="store_true",
                        help="Save test log output on success.")
    parser.add_argument('--list', dest="list_tests", action="store_true", default=False,
                        help="Print list of tests instead of executing them")
    parser.add_argument('--skip', default="",
                        dest="skip_pattern", action="store",
                        help="Skip tests which match the provided pattern")
    args = parser.parse_args()

    if not output_is_a_tty:
        args.verbose = True

    if not args.modes:
        try:
            out = subprocess.Popen(['ninja', 'mode_list'], stdout=subprocess.PIPE).communicate()[0].decode()
            # [1/1] List configured modes
            # debug release dev
            args.modes= re.sub(r'.* List configured modes\n(.*)\n', r'\1', out, 1, re.DOTALL).split("\n")[-1].split(' ')
        except Exception as e:
            print(palette.fail("Failed to read output of `ninja mode_list`: please run ./configure.py first"))
            raise

    def prepare_dir(dirname, pattern):
        # Ensure the dir exists
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        # Remove old artefacts
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
    except Exception as e:
        print(palette.fail("Failed to read output of `ninja unit_test_list`: please run ./configure.py first"))
        raise

    return args


def find_tests(options):

    for f in glob.glob(os.path.join("test", "*")):
        if os.path.isdir(f) and os.path.isfile(os.path.join(f, "suite.yaml")):
            for mode in options.modes:
                suite = TestSuite.opt_create(f)
                suite.add_test_list(mode, options)

    if not TestSuite.test_count():
        if len(options.name):
            print("Test {} not found".format(palette.path(options.name[0])))
            sys.exit(1)
        else:
            print(palette.warn("No tests found. Please enable tests in ./configure.py first."))
            sys.exit(0)

    logging.info("Found %d tests, repeat count is %d, starting %d concurrent jobs",
                 TestSuite.test_count(), options.repeat, options.jobs)


async def run_all_tests(signaled, options):
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
        for test in TestSuite.tests():
            # +1 for 'signaled' event
            if len(pending) > options.jobs:
                # Wait for some task to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                await reap(done, pending, signaled)
            pending.add(asyncio.create_task(test.run(options)))
        # Wait & reap ALL tasks but signaled_task
        # Do not use asyncio.ALL_COMPLETED to print a nice progress report
        while len(pending) > 1:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await reap(done, pending, signaled)

    except asyncio.CancelledError:
        return

    console.print_end_blurb()


def read_log(log_filename):
    """Intelligently read test log output"""
    try:
        with open(log_filename, "r") as log:
            msg = log.read()
            return msg if len(msg) else "===Empty log output==="
    except FileNotFoundError:
        return "===Log {} not found===".format(log_filename)
    except OSError as e:
        return "===Error reading log {}===".format(e)


def print_summary(failed_tests):
    if failed_tests:
        print("The following test(s) have failed: {}".format(
            palette.path(" ".join([t.name for t in failed_tests]))))
        if not output_is_a_tty:
            for test in failed_tests:
                test.print_summary()
                print("-"*78)
        print("Summary: {} of the total {} tests failed".format(
            len(failed_tests), TestSuite.test_count()))


def print_unidiff(fromfile, tofile):
    with open(fromfile, "r") as frm, open(tofile, "r") as to:
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
            sys.stdout.write(line)


def write_junit_report(tmpdir, mode):
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
            xml_fail = ET.SubElement(xml_res, 'failure')
            xml_fail.text = "Test {} {} failed, check the log at {}".format(
                test.path,
                " ".join(test.args),
                test.log_filename)
    if total == 0:
        return
    xml_results.set("tests", str(total))
    xml_results.set("failures", str(failed))
    with open(junit_filename, "w") as f:
        ET.ElementTree(xml_results).write(f, encoding="unicode")


def open_log(tmpdir):
    pathlib.Path(tmpdir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(tmpdir, "test.py.log"),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s> %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.critical("Started %s", " ".join(sys.argv))


async def main():

    options = parse_cmd_line()

    open_log(options.tmpdir)

    find_tests(options)
    if options.list_tests:
        print('\n'.join([t.name for t in TestSuite.tests()]))
        return 0

    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_event_loop(), signaled)

    await run_all_tests(signaled, options)

    if signaled.is_set():
        return -signaled.signo

    failed_tests = [t for t in TestSuite.tests() if t.success is not True]

    print_summary(failed_tests)

    for mode in options.modes:
        write_junit_report(options.tmpdir, mode)

    if 'coverage' in options.modes:
        coverage.generate_coverage_report("build/coverage", "tests")

    # Note: failure codes must be in the ranges 0-124, 126-127,
    #       to cooperate with git bisect's expectations
    return 0 if not failed_tests else 1

if __name__ == "__main__":
    colorama.init()

    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
