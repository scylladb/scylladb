#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import argparse
import asyncio
import hashlib
import shlex
import socket
from random import randint

from types import SimpleNamespace

import colorama
import glob
import itertools
import logging
import multiprocessing
import os
import pathlib
import re
import resource
import signal
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING, Any

import humanfriendly
import treelib

from scripts import coverage
from test import ALL_MODES, TOP_SRC_DIR, path_to, TEST_DIR
from test.pylib import coverage_utils
from test.pylib.suite.base import (
    Test,
    TestSuite,
    init_testsuite_globals,
    output_is_a_tty,
    palette,
    prepare_dirs,
    start_3rd_party_services,
)
from test.pylib.resource_gather import run_resource_watcher
from test.pylib.util import LogPrefixAdapter, get_configured_modes, ninja

if TYPE_CHECKING:
    from typing import List

PYTEST_RUNNER_DIRECTORIES = [TEST_DIR / 'boost', TEST_DIR / 'ldap', TEST_DIR / 'raft', TEST_DIR / 'unit']

launch_time = time.monotonic()


class TabularConsoleOutput:
    """Print test progress to the console"""

    def __init__(self, verbose: bool, test_count: int) -> None:
        self.verbose = verbose
        self.test_count = test_count
        self.last_test_no = 0

    def print_start_blurb(self) -> None:
        print("="*80)
        print("{:10s} {:^8s} {:^7s} {:8s} {}".format("[N/TOTAL]", "SUITE", "MODE", "RESULT", "TEST"))
        print("-"*78)
        print("")

    def print_end_blurb(self) -> None:
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
            f"[{self.last_test_no}/{self.test_count}]",
            test.suite.name, test.mode[:7],
            status,
            test.uname
        )
        if not self.verbose:
            if test.success:
                print("\033[A", end="\r")
                print("\033[K", end="\r")
                print(msg)
            else:
                print("\033[A", end="\r")
                print("\033[K", end="\r")
                print(f"{msg}\n")
        else:
            msg += " {:.2f}s".format(test.time_end - test.time_start)
            print(msg)


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
    parser.add_argument("--tmpdir", action="store", default=str(TOP_SRC_DIR / "testlog"),
                        help="Path to temporary test data and log files.  The data is further segregated per build mode.")
    parser.add_argument("--gather-metrics", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-failures", type=int, default=0,
                        help="Maximum number of failures to tolerate before cancelling rest of tests.")
    parser.add_argument('--mode', choices=ALL_MODES, action="append", dest="modes",
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
    parser.add_argument('--manual-execution', action='store_true', default=False,
                        help='Let me manually run the test executable at the moment this script would run it')
    parser.add_argument('--byte-limit', action="store", default=randint(0, 2000), type=int,
                        help="Specific byte limit for failure injection (random by default)")
    parser.add_argument('--skip-internet-dependent-tests', action="store_true",
                        help="Skip tests which depend on artifacts from the internet.")
    parser.add_argument("--pytest-arg", action='store', type=str,
                        default=None, dest="pytest_arg",
                        help="Additional command line arguments to pass to pytest, for example ./test.py --pytest-arg=\"-v -x\"")
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
            args.modes = get_configured_modes()
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

    args.tmpdir = os.path.abspath(args.tmpdir)
    prepare_dirs(tempdir_base=pathlib.Path(args.tmpdir), modes=args.modes, gather_metrics=args.gather_metrics)

    if args.extra_scylla_cmdline_options:
        args.extra_scylla_cmdline_options = args.extra_scylla_cmdline_options.split()

    return args


async def find_tests(options: argparse.Namespace) -> None:

    for f in glob.glob(os.path.join("test", "*")):
        if os.path.isdir(f) and os.path.isfile(os.path.join(f, "suite.yaml")):
            for mode in options.modes:
                suite = TestSuite.opt_create(f, options, mode)
                await suite.add_test_list()


def run_pytest(options: argparse.Namespace, run_id: int) -> tuple[int, list[SimpleNamespace]]:
    # When tests are executed in parallel on different hosts, we need to distinguish results from them.
    # So this host_id needed to not overwrite results from different hosts during Jenkins will copy to one directory.
    hostname = socket.gethostname()
    host_id = hashlib.sha3_224((hostname + str(time.time())).encode('utf-8')).hexdigest()[:5]
    failed_tests = []
    temp_dir = pathlib.Path(options.tmpdir).absolute()
    report_dir =  temp_dir / 'report'
    junit_output_file = report_dir / f'pytest_cpp_{run_id}_{host_id}.xml'
    files_to_run = []
    test_names = []
    for name in options.name:
        file_name = name
        if '::' in name:
            file_name, _ = name.split('::', maxsplit=1)
        if any((TOP_SRC_DIR / file_name).is_relative_to(x) for x in PYTEST_RUNNER_DIRECTORIES):
            files_to_run.append(name)
    if not options.name:
        files_to_run = [str(directory) for directory in PYTEST_RUNNER_DIRECTORIES]
    if not files_to_run:
        logging.info(f'No boost found. Skipping pytest execution for boost tests.')
        return 0, []
    expression = ' or '.join(test_names)
    if options.skip_patterns:
        expression += f'{"and not ".join(options.skip_patterns)}'
    modes = ' '.join(f'--mode={mode}' for mode in options.modes)
    args = [
        'pytest',
        "-s",  # don't capture print() output inside pytest
        '--color=yes',
        modes,
    ]
    if options.list_tests:
        args.extend(['--collect-only', '--quiet'])
    else:
        args.extend([
            "--log-level=DEBUG",  # Capture logs
            f'--junit-xml={junit_output_file}',
            "-rf",
            f'-n{int(options.jobs)}',
            f'--tmpdir={temp_dir}',
            f'--run_id={run_id}',
            f'--maxfail={options.max_failures}',
            f'--alluredir={report_dir / f"allure_{host_id}"}',
            '-v' if options.verbose else '-q',
        ])
    if options.pytest_arg:
        # If pytest_arg is provided, it should be a string with arguments to pass to pytest
        args.extend(shlex.split(options.pytest_arg))
    if options.random_seed:
        args.append(f'--random-seed={options.random_seed}')
    if options.gather_metrics:
        args.append('--gather-metrics')
    if len(expression) > 1:
        args.extend(['-k', expression])
    if not options.save_log_on_success:
        args.append('--allure-no-capture')
    else:
        args.append('--save-log-on-success')
    if options.markers:
        args.append(f"-m={options.markers}")
    args.extend(files_to_run)

    args = shlex.split(' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, universal_newlines=True)
    try:
        # Read output from pytest and print it to the console
        if options.verbose:
            for line in p.stdout:
                print(line, end='', flush=True)
        else:
            # without verbose, pytest output only one line, so to have live progress, need to read it by char,
            # because each char is a test result
            while True:
                char = p.stdout.read(1)
                if char == '' and p.poll() is not None:
                    break
                if char:
                    print(char, end='', flush=True)

        # Wait for pytest to finish and get its return code
        p.wait(timeout=60)
    except subprocess.TimeoutExpired:
        print('Timeout reached')
        p.kill()
    except KeyboardInterrupt:
        p.kill()
        raise

    if options.list_tests:
        return 0, []

    suite = ET.parse(junit_output_file).getroot().find('testsuite')
    total_tests = int(suite.get('tests'))

    # Find failed tests in the pytest XML output, create a SimpleNamespace for each to mimic the Test class to be able
    # to print the summary later
    for test_case in suite.findall('testcase'):
        if test_case.find('error') is not None or test_case.find('failure') is not None:
            classname = test_case.get('classname')
            if classname.endswith('.cc'):
                file_path, extension = test_case.get('classname').rsplit('.', 1)
            else:
                extension = 'py'
                file_path = '/'.join(x for x in test_case.get('classname').split('.') if x.islower())
            # get the test name without mode name
            test_name = test_case.get('name').split('.')[0]
            test = SimpleNamespace()

            test.name = f"test/{file_path.replace('.', '/')}.{extension}::{test_name}"
            # print_summary used to print additional information about the failed test that is called in verbose mode
            # This is needed to mimic the Test class and not fail the run summary output
            test.print_summary = Test.print_summary

            failed_tests.append(test)

    return total_tests, failed_tests



async def run_all_tests(signaled: asyncio.Event, options: argparse.Namespace) -> tuple[int | Any, list[
    SimpleNamespace]] | None:
    failed_tests = []
    console = TabularConsoleOutput(options.verbose, TestSuite.test_count())
    signaled_task = asyncio.create_task(signaled.wait())
    pending = {signaled_task}

    async def cancel(pending, msg):
        for task in pending:
            task.cancel(msg)
        await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
        print("...done")

    async def reap(done, pending, signaled):
        nonlocal console
        if signaled.is_set():
            await cancel(pending, "Signal received")
        failed = 0
        for coro in done:
            result = coro.result()
            if isinstance(result, bool):
                continue    # skip signaled task result
            if not result.success:
                failed += 1
            if not result.did_not_run:
                console.print_progress(result)
        return failed

    total_tests = 0
    max_failures = options.max_failures
    failed = 0
    try:
        await start_3rd_party_services(tempdir_base=pathlib.Path(options.tmpdir), toxiproxy_byte_limit=options.byte_limit)
        for i in range(1, options.repeat + 1):
            result = run_pytest(options, run_id=i)
            total_tests += result[0]
            failed_tests.extend(result[1])
            if len(failed_tests) >= max_failures != 0:
                print("Too much failures, stopping")
                break
        console.print_start_blurb()
        TestSuite.artifacts.add_exit_artifact(None, TestSuite.hosts.cleanup)
        for test in TestSuite.all_tests():
            # +1 for 'signaled' event
            if len(pending) > options.jobs:
                # Wait for some task to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                failed += await reap(done, pending, signaled)
                if max_failures != 0 and max_failures <= failed:
                    print("Too much failures, stopping")
                    await cancel(pending, "Too much failures, stopping")
            pending.add(asyncio.create_task(test.suite.run(test, options)))
        # Wait & reap ALL tasks but signaled_task
        # Do not use asyncio.ALL_COMPLETED to print a nice progress report
        while len(pending) > 1:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            failed += await reap(done, pending, signaled)
            if max_failures != 0 and max_failures <= failed:
                print("Too much failures, stopping")
                await cancel(pending, "Too much failures, stopping")
    finally:
        await TestSuite.artifacts.cleanup_before_exit()

    console.print_end_blurb()
    return total_tests, failed_tests


def print_summary(failed_tests: List["Test"], cancelled_tests: int, options: argparse.Namespace, failed_pytest_tests: list[SimpleNamespace], total_tests_pytest: int) -> None:
    rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_used = rusage.ru_stime + rusage.ru_utime
    cpu_available = (time.monotonic() - launch_time) * multiprocessing.cpu_count()
    utilization = cpu_used / cpu_available
    print(f"CPU utilization: {utilization*100:.1f}%")
    if failed_tests or failed_pytest_tests:
        all_fails = [*failed_tests, *failed_pytest_tests]
        total_tests = TestSuite.test_count() + total_tests_pytest
        print(f'The following test(s) have failed: {" ".join(t.name for t in all_fails)}')
        if options.verbose:
            for test in failed_tests:
                test.print_summary()
                print("-"*78)
        if cancelled_tests > 0:
            print(f"Summary: {len(all_fails)} of the total {total_tests} tests failed, {cancelled_tests} cancelled")
        else:
            print(f"Summary: {len(all_fails)} of the total {total_tests} tests failed")


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

    init_testsuite_globals()

    await find_tests(options)
    if options.list_tests:
        print('\n'.join([f"{t.suite.mode:<8} {type(t.suite).__name__[:-9]:<11} {t.name}"
                         for t in TestSuite.all_tests()]))
        run_pytest(options, run_id=1)
        return 0

    if options.manual_execution and TestSuite.test_count() > 1:
        print('--manual-execution only supports running a single test, but multiple selected: {}'.format(
            [t.path for t in TestSuite.tests()][:3])) # Print whole t.path; same shortname may be in different dirs.
        return 1

    signaled = asyncio.Event()
    stop_event = asyncio.Event()
    resource_watcher = run_resource_watcher(options.gather_metrics, signaled, stop_event, options.tmpdir)

    setup_signal_handlers(asyncio.get_running_loop(), signaled)

    try:
        logging.info('running all tests')
        total_tests_pytest, failed_pytest_tests = await run_all_tests(signaled, options)
        logging.info('after running all tests')
        stop_event.set()
        async with asyncio.timeout(5):
            await resource_watcher
    except asyncio.CancelledError:
        print('\ntests cancelled by signal')
        return 1
    except Exception as e:
        print(palette.fail(e))
        raise

    if signaled.is_set():
        return -signaled.signo      # type: ignore

    failed_tests = [test for test in TestSuite.all_tests() if test.failed]
    cancelled_tests = sum(1 for test in TestSuite.all_tests() if test.did_not_run)

    print_summary(failed_tests, cancelled_tests, options, failed_pytest_tests, total_tests_pytest)

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


if __name__ == "__main__":
    colorama.init()
    # gh-16583: ignore the inherited client host's ScyllaDB environment,
    # since it may break the tests
    if "SCYLLA_CONF" in os.environ:
        del os.environ["SCYLLA_CONF"]
    if "SCYLLA_HOME" in os.environ:
        del os.environ["SCYLLA_HOME"]

    if sys.version_info < (3, 11):
        print("Python 3.11 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
