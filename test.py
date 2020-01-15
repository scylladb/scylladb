#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 ScyllaDB
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
import shlex
import shutil
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
import yaml


def create_formatter(*decorators):
    """Return a function which decorates its argument with the given
    color/style if stdout is a tty, and leaves intact otherwise."""
    def color(arg):
        return "".join(decorators) + str(arg) + colorama.Style.RESET_ALL

    def nocolor(arg):
        return str(arg)
    return color if os.isatty(sys.stdout.fileno()) else nocolor


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
        lst = glob.glob(os.path.join(self.path, self.pattern))
        if lst:
            lst.sort()
        long_tests = set(self.cfg.get("long", []))
        for t in lst:
            shortname = os.path.splitext(os.path.basename(t))[0]
            if mode not in ["release", "dev"] and shortname in long_tests:
                continue
            t = os.path.join(self.name, shortname)
            patterns = options.name if options.name else [t]
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


class UnitTest(Test):
    standard_args = shlex.split("--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000 --collectd 0")

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, suite, mode, options)
        self.path = os.path.join("build", self.mode, "test", self.name)
        self.args = shlex.split(args) + UnitTest.standard_args

    def print_summary(self):
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options):
        self.success = await run_test(self, options)
        logging.info("Test #%d %s", self.id, "succeeded" if self.success else "failed ")
        return self


class BoostTest(UnitTest):
    """A unit test which can produce its own XML output"""

    def __init__(self, test_no, shortname, args, suite, mode, options):
        super().__init__(test_no, shortname, args, suite, mode, options)
        boost_args = []
        xmlout = os.path.join(options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        boost_args += ['--report_level=no', '--logger=HRF,test_suite:XML,test_suite,' + xmlout]
        boost_args += ['--']
        self.args = boost_args + self.args


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
        self.args = shlex.split("-c2 -m2G --input={} --output={} --log={}".format(
            self.cql, self.tmpfile, self.log_filename))
        self.args += UnitTest.standard_args
        self.is_executed_ok = False
        self.is_new = False
        self.is_equal_result = None
        self.summary = "not run"

    async def run(self, options):
        self.is_executed_ok = await run_test(self, options)
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


def print_start_blurb():
    print("="*80)
    print("{:7s} {:50s} {:^8s} {:8s}".format("[N/TOTAL]", "TEST", "MODE", "RESULT"))
    print("-"*78)


def print_end_blurb(verbose):
    if not verbose:
        sys.stdout.write('\n')
    print("-"*78)


def print_progress(test, cookie, verbose):
    if isinstance(cookie, int):
        cookie = (0, 1, cookie)

    last_len, n, n_total = cookie
    msg = "{:9s} {:50s} {:^8s} {:8s}".format(
        "[{}/{}]".format(n, n_total),
        test.name, test.mode[:8],
        palette.ok("[ PASS ]") if test.success else palette.fail("[ FAIL ]")
    )
    if verbose is False:
        print('\r' + ' ' * last_len, end='')
        last_len = len(msg)
        print('\r' + msg, end='')
    else:
        print(msg)

    return (last_len, n + 1, n_total)


async def run_test(test, options):
    """Run test program, return True if success else False"""
    file = io.StringIO()

    def report_error(out):
        print('=== stdout START ===', file=file)
        print(out, file=file)
        print('=== stdout END ===', file=file)
    process = None
    stdout = None
    logging.info("Starting test #%d: %s %s", test.id, test.path, " ".join(test.args))
    try:
        with open(test.log_filename, "wb") as log:
            process = await asyncio.create_subprocess_exec(
                test.path,
                *test.args,
                stderr=log,
                stdout=log,
                env=dict(os.environ,
                         UBSAN_OPTIONS='halt_on_error=1:abort_on_error=1',
                         ASAN_OPTIONS='disable_coredump=0:abort_on_error=1',
                         BOOST_TEST_CATCH_SYSTEM_ERRORS="no"),
                preexec_fn=os.setsid,
            )
        stdout, _ = await asyncio.wait_for(process.communicate(), options.timeout)
        if process.returncode != 0:
            print('  with error code {code}\n'.format(code=process.returncode), file=file)
            report_error(stdout.decode(encoding='UTF-8'))
        return process.returncode == 0
    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        if process is not None:
            process.kill()
            stdout, _ = await process.communicate()
        if isinstance(e, asyncio.TimeoutError):
            print('  timed out', file=file)
            report_error(stdout.decode(encoding='UTF-8') if stdout else "No output")
        elif isinstance(e, asyncio.CancelledError):
            print(test.name, end=" ")
    except Exception as e:
        print('  with error {e}\n'.format(e=e), file=file)
        report_error(e)
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
    all_modes = ['debug', 'release', 'dev', 'sanitize']
    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
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
    parser.add_argument('--timeout', action="store", default="3000", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    args = parser.parse_args()

    if not sys.stdout.isatty():
        args.verbose = True

    if not args.modes:
        out = subprocess.Popen(['ninja', 'mode_list'], stdout=subprocess.PIPE).communicate()[0].decode()
        # [1/1] List configured modes
        # debug release dev
        args.modes = out.split('\n')[1].split(' ')

    def prepare_dir(dirname, pattern):
        # Ensure the dir exists
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        # Remove old artefacts
        for p in glob.glob(os.path.join(dirname, pattern), recursive=True):
            pathlib.Path(p).unlink()

    args.tmpdir = os.path.abspath(args.tmpdir)
    prepare_dir(args.tmpdir, "*.log")

    for mode in args.modes:
        prepare_dir(os.path.join(args.tmpdir, mode), "*.{log,reject}")
        prepare_dir(os.path.join(args.tmpdir, mode, "xml"), "*.xml")

    return args


def find_tests(options):

    for f in glob.glob(os.path.join("test", "*")):
        if os.path.isdir(f) and os.path.isfile(os.path.join(f, "suite.yaml")):
            for mode in options.modes:
                suite = TestSuite.opt_create(f)
                suite.add_test_list(mode, options)

    if not TestSuite.test_count():
        print("Test {} not found".format(palette.path(options.name[0])))
        sys.exit(1)

    logging.info("Found %d tests, repeat count is %d, starting %d concurrent jobs",
                 TestSuite.test_count(), options.repeat, options.jobs)


async def run_all_tests(signaled, options):
    cookie = TestSuite.test_count()
    signaled_task = asyncio.create_task(signaled.wait())
    pending = set([signaled_task])

    async def cancel(pending):
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        print("... done.")
        raise asyncio.CancelledError

    async def reap(done, pending, signaled):
        nonlocal cookie
        if signaled.is_set():
            await cancel(pending)
        for coro in done:
            result = coro.result()
            if isinstance(result, bool):
                continue    # skip signaled task result
            cookie = print_progress(result, cookie, options.verbose)
    print_start_blurb()
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

    print_end_blurb(options.verbose)


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
            xml_fail.text = "Test {} {} failed:\n".format(test.path, " ".join(test.args))
            xml_fail.text += read_log(test.log_filename)
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
    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_event_loop(), signaled)

    await run_all_tests(signaled, options)

    if signaled.is_set():
        return -signaled.signo

    failed_tests = [t for t in TestSuite.tests() if t.success is not True]

    print_summary(failed_tests)

    for mode in options.modes:
        write_junit_report(options.tmpdir, mode)

    return 0 if not failed_tests else -1

if __name__ == "__main__":
    colorama.init()

    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
