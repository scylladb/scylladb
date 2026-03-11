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
import shlex
import textwrap
from random import randint

import pytest

import colorama
import itertools
import logging
import multiprocessing
import os
import pathlib
import resource
import subprocess
import sys
import time

import humanfriendly
import treelib

from scripts import coverage
from test import ALL_MODES, HOST_ID, TOP_SRC_DIR, path_to
from test.pylib import coverage_utils
from test.pylib.suite.base import (
    TestSuite,
    palette,
)
from test.pylib.util import LogPrefixAdapter, get_configured_modes

launch_time = time.monotonic()


def parse_cmd_line() -> argparse.Namespace:
    """ Print usage and process command line options. """
    parser = argparse.ArgumentParser(description='Scylla test runner', formatter_class=argparse.RawTextHelpFormatter)

    name_help = textwrap.dedent("""\
        Can be empty. List of test names or path to test files, to look for.
        
        provide the path to the test file for execution or path to the directory
        to narrow you can use function name 'test/boost/aggregate_fcts_test.cc::test_aggregate_avg'
        """)

    parser.add_argument(
        "name",
        nargs="*",
        action="store",
        help=name_help,
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
                        help="timeout value for single test execution")
    parser.add_argument('--session-timeout', action="store", default="24000", type=int,
                        help="timeout value for test.py/pytest session execution")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--quiet', '-q', action='store_true', default=False,
                        help='Quiet reporting')
    parser.add_argument('--jobs', '-j', action="store", type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument('--save-log-on-success', "-s", default=False,
                        dest="save_log_on_success", action="store_true",
                        help="Save test log output on success and skip cleanup before the run.")
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
    parser.add_argument('-k', metavar="EXPRESSION", action="store",
                        help="Only run tests which match the given substring expression. An expression is a Python evaluable expression where all names are "
                        "substring-matched against test names and their parent classes. Example: -k 'test_method or test_other' matches all test functions and "
                        "classes whose name contains 'test_method' or 'test_other', while -k 'not test_method' matches those that don't contain 'test_method' "
                        "in their names. -k 'not test_method and not test_other' will eliminate the matches. Additionally keywords are matched to classes and "
                        "functions containing extra names in their 'extra_keyword_matches' set, as well as functions which have names assigned directly to "
                        "them. The matching is case-insensitive.")
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
    parser.add_argument('--exe-path', default=False,
                     dest="exe_path", action="store",
                     help="Path to the executable to run. Not working with `mode`")
    parser.add_argument('--exe-url', default=False,
                     dest="exe_url", action="store",
                     help="URL to download the relocatable executable. Not working with `mode`")
    scylla_additional_options = parser.add_argument_group('Additional options for Scylla tests')
    scylla_additional_options.add_argument('--extra-scylla-cmdline-options', action="store", default="", type=str,
                                           help="Passing extra scylla cmdline options for all tests. Options should be space separated:"
                                                "'--logger-log-level raft=trace --default-log-level error'")

    boost_group = parser.add_argument_group('boost suite options')
    boost_group.add_argument('--random-seed', action="store",
                             help="Random number generator seed to be used by boost tests")

    args = parser.parse_args()

    if args.skip_patterns and args.k:
        parser.error(palette.fail('arguments --skip and -k are mutually exclusive, please use only one of them'))

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

    return args


def run_pytest(options: argparse.Namespace) -> int:
    # When tests are executed in parallel on different hosts, we need to distinguish results from them.
    # So HOST_ID needed to not overwrite results from different hosts during Jenkins will copy to one directory.

    temp_dir = pathlib.Path(options.tmpdir).absolute()

    report_dir =  temp_dir / 'report'
    junit_output_file = report_dir / f'pytest_cpp_{HOST_ID}.xml'
    files_to_run = options.name or [str(TOP_SRC_DIR / 'test/')]
    args = [
        '--color=yes',
        f'--repeat={options.repeat}',
        *[f'--mode={mode}' for mode in options.modes],
    ]
    if options.list_tests:
        args.extend(['--collect-only', '--quiet', '--no-header'])
    else:
        threads = int(options.jobs)
        # debug mode is very CPU and memory hungry, so we need to lower the number of threads to be able to finish tests
        if 'debug' in options.modes:
            threads = int(threads * 0.5)
        args.extend([
            "--log-level=DEBUG",  # Capture logs
            f'--junit-xml={junit_output_file}',
            "-rf",
            f'-n{threads}',
            f'--tmpdir={temp_dir}',
            f'--maxfail={options.max_failures}',
            f'--alluredir={report_dir / f"allure_{HOST_ID}"}',
            f'--dist=worksteal',
        ])
    if options.verbose:
        args.append('-v')
    if options.quiet:
        args.append('--quiet')
        args.extend(['-p','no:sugar'])
    if options.pytest_arg:
        # If pytest_arg is provided, it should be a string with arguments to pass to pytest
        args.extend(shlex.split(options.pytest_arg))
    if options.random_seed:
        args.append(f'--random-seed={options.random_seed}')
    if options.gather_metrics:
        args.append('--gather-metrics')
    if options.timeout:
        args.append(f'--timeout={options.timeout}')
    if options.session_timeout:
        args.append(f'--session-timeout={options.session_timeout}')
    if options.skip_patterns:
        args.append(f'-k={" and ".join([f"not {pattern}" for pattern in options.skip_patterns])}')
    if options.k:
        args.append(f'-k={options.k}')
    if options.extra_scylla_cmdline_options:
        args.append(f'--extra-scylla-cmdline-options={options.extra_scylla_cmdline_options}')
    if not options.save_log_on_success:
        args.append('--allure-no-capture')
    else:
        args.append('--save-log-on-success')
    if options.markers:
        args.append(f'-m={options.markers}')
    args.extend(files_to_run)
    exit_code = pytest.main(args=args)

    rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_used = rusage.ru_stime + rusage.ru_utime
    cpu_available = (time.monotonic() - launch_time) * multiprocessing.cpu_count()
    print(f"CPU utilization: {cpu_used / cpu_available * 100:.1f}%")

    return exit_code


async def main() -> int:

    options = parse_cmd_line()

    if options.list_tests:
        return run_pytest(options)

    try:
        logging.info('running all tests')
        # Run pytest in the default thread pool executor so the event loop stays
        # responsive (e.g. signal handlers continue to work while pytest runs).
        loop = asyncio.get_running_loop()
        exit_code = await loop.run_in_executor(None, run_pytest, options)
        logging.info('after running all tests')
    except asyncio.CancelledError:
        print('\ntests cancelled by signal')
        return 1
    except Exception as e:
        print(palette.fail(e))
        raise

    if 'coverage' in options.modes:
        coverage.generate_coverage_report(path_to("coverage", "tests"))

    if options.coverage:
        await process_coverage(options)

    # Note: failure codes must be in the ranges 0-124, 126-127,
    #       to cooperate with git bisect's expectations
    return exit_code


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
