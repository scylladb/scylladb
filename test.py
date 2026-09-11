#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-present ScyllaDB
#
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import asyncio
import math
import textwrap
from random import randint

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
from test import ALL_MODES, TOP_SRC_DIR, path_to, DEBUG_MODES
from test.pylib import coverage_utils
from test.pylib.scheduling import execution, registry
from test.pylib.scheduling.config import RunConfig, SchedulerError
from test.pylib.scheduling.scheduler import Scheduler
from test.pylib.util import LogPrefixAdapter, get_configured_modes, palette

launch_time = time.monotonic()

class ThreadsCalculator:
    """
    The ThreadsCalculator class calculates the number of jobs that can be run concurrently based on system
    memory and CPU constraints. It allows resource reservation and configurable parameters for
    flexible job scheduling in various modes, such as `debug`.
    """

    def __init__(self,
                 modes: list[str],
                 threads_multiplier: float = 1.0,
                 min_system_memory_reserve: float = 5e9,
                 max_system_memory_reserve: float = 8e9,
                 system_memory_reserve_fraction = 16,
                 max_test_memory: float = 5e9,
                 test_memory_fraction: float = 8.0,
                 debug_test_memory_multiplier: float = 1.5,
                 debug_cpus_per_test_job=1.5,
                 non_debug_cpus_per_test_job: float =1.0,
                 non_debug_max_test_memory: float = 4e9
                 ):
        sys_mem = int(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
        system_memory_reserve = int(min(
            max(sys_mem / system_memory_reserve_fraction, min_system_memory_reserve),
            max_system_memory_reserve,
        ))
        available_mem = max(0, sys_mem - system_memory_reserve)
        is_debug = set(DEBUG_MODES) & set(modes)
        test_mem = min(
            sys_mem / test_memory_fraction,
            max_test_memory if is_debug else non_debug_max_test_memory,
        )
        if is_debug:
            test_mem *= debug_test_memory_multiplier
        self.cpus_per_test_job = (
            debug_cpus_per_test_job if is_debug else non_debug_cpus_per_test_job
        )
        self.default_num_jobs_mem = max(1, int(available_mem // test_mem))
        self.threadt_multiplier = threads_multiplier

    def get_number_of_threads(self, nr_cpus: int) -> int:
        default_num_jobs_cpu = max(1, math.ceil(nr_cpus / self.cpus_per_test_job))
        return int(min(self.default_num_jobs_mem, default_num_jobs_cpu) * self.threadt_multiplier)



class SelectScheduler(argparse.Action):
    """``--scheduler=<name>``, or ``--scheduler=list`` to print the registry.

    Both the listing and the "no such scheduler" error live on the option, so
    that a bad name is reported the way argparse reports any other bad value
    instead of being re-checked in the middle of parse_cmd_line().
    """

    def __call__(self, parser, namespace, value, option_string=None) -> None:
        if value == registry.LIST_KEYWORD:
            print(registry.describe())
            parser.exit()
        if value not in registry.SCHEDULERS:
            parser.error(palette.fail(
                f"unknown --scheduler={value}; available: {', '.join(registry.known_names())} "
                f"(use --scheduler={registry.LIST_KEYWORD} for details)"))
        setattr(namespace, self.dest, value)


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
    parser.add_argument('--timeout', action="store", default="3600", type=int,
                        help="timeout value for single test execution")
    parser.add_argument('--session-timeout', action="store", default="24000", type=int,
                        help="timeout value for test.py/pytest session execution")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--quiet', '-q', action='store_true', default=False,
                        help='Quiet reporting')
    threads = parser.add_mutually_exclusive_group(required=False)
    threads.add_argument('--jobs', '-j', action="store", type=int,
                        help="Number of jobs to use for running the tests")
    threads.add_argument('--threads-multiplier', type=float, default=1.0,
                         dest="threads_multiplier", action="store",
                         help="Multiplier for the number of threads to use for running the tests. Default is) 1.0, "
                              "which means no change. Use a value less than 1.0 to reduce the number of threads, or a"
                              "value greater than 1.0 to increase the number of threads.")
    parser.add_argument('--scheduler', action=SelectScheduler, default=registry.DEFAULT, metavar="NAME",
                        help=f"Which scheduler decides how the tests are spread over this machine. "
                             f"Default: {registry.DEFAULT}, which makes no decisions and hands the run to "
                             f"pytest-xdist. Use --scheduler={registry.LIST_KEYWORD} to see what is available.")
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
    parser.add_argument('--manual-execution', action='store_true', default=False,
                        help='Let me manually run the test executable at the moment this script would run it')
    parser.add_argument('--byte-limit', action="store", default=randint(0, 2000), type=int,
                        help="Specific byte limit for failure injection (random by default)")
    parser.add_argument('--skip-internet-dependent-tests', action="store_true",
                        help="Skip tests which depend on artifacts from the internet.")
    parser.add_argument('--keep-duplicates', action='store_true', default=False,
                        help="Do not deduplicate test arguments.")
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

    if args.exe_path or args.exe_url:
        if args.modes:
            parser.error(palette.fail('arguments --exe-path/--exe-url and --mode are mutually exclusive, please use only one of them'))
        # The executable under test is given explicitly, so no configured
        # build is required; the pytest runner derives the "custom_exe" mode
        # from the executable options.
        args.modes = []
    elif not args.modes:
        try:
            args.modes = get_configured_modes()
        except Exception:
            print(palette.fail("Failed to read output of `ninja mode_list`: please run ./configure.py first"))
            raise

    if not args.jobs:
        if not args.cpus:
            nr_cpus = multiprocessing.cpu_count()
        else:
            nr_cpus = int(subprocess.check_output(
                ['taskset', '-c', args.cpus, 'python3', '-c',
                 'import os; print(len(os.sched_getaffinity(0)))']))
        # ThreadsCalculator is passthrough's calculation, not a shared
        # foundation: a scheduler that wants a different number brings its own.
        args.jobs = ThreadsCalculator(args.modes, args.threads_multiplier).get_number_of_threads(nr_cpus)

    if not args.coverage_modes and args.coverage:
        args.coverage_modes = list(args.modes)
        if not args.coverage_modes:
            args.coverage = False
    elif args.coverage_modes:
        missing_coverage_modes = set(args.coverage_modes).difference(set(args.modes))
        if len(missing_coverage_modes) > 0:
            raise RuntimeError(f"The following modes weren't built or ran (using the '--mode' option): {missing_coverage_modes}")
        args.coverage = True

    args.tmpdir = os.path.abspath(args.tmpdir)

    return args


#: The selected scheduler refused to schedule this run.  test.py otherwise
#: returns pytest's exit codes verbatim, so this reuses one rather than
#: inventing a meaning: a scheduler only ever sees the command line, so refusing
#: it is a usage error.  A code of our own would have had to avoid pytest's
#: 0-5 as well as EXIT_MAXFAIL_REACHED.
EXIT_SCHEDULER_ERROR = execution.EXIT_USAGE_ERROR


def select_scheduler(options: argparse.Namespace) -> tuple[Scheduler, RunConfig]:
    """Pick the scheduler, build the run configuration, and let it decide.

    Three objects, each with one job: the scheduler decides, the config holds
    what was decided, the execution module carries it out.  test.py answers no
    question about what kind of scheduler it has — it makes one configure()
    call and hands the result over.

    Raises SchedulerError if the scheduler refuses to schedule this run; the
    scheduler owns that message, since only it knows what it could not honour.
    """
    scheduler = registry.get_scheduler(options.scheduler)
    cfg = RunConfig.defaults(options)
    if options.list_tests:
        # Listing decides nothing, so there is nothing to schedule.
        return scheduler, cfg
    scheduler.configure(cfg)
    cfg.log_scheduler(scheduler)
    return scheduler, cfg


def run_pytest(cfg: RunConfig, scheduler: Scheduler) -> int:
    exit_code = execution.run_pytest(cfg, scheduler)

    rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_used = rusage.ru_stime + rusage.ru_utime
    cpu_available = (time.monotonic() - launch_time) * multiprocessing.cpu_count()
    print(f"CPU utilization: {cpu_used / cpu_available * 100:.1f}%")

    return exit_code


async def main() -> int:

    options = parse_cmd_line()
    try:
        scheduler, cfg = select_scheduler(options)
    except SchedulerError as e:
        print(palette.fail(f"error: --scheduler={options.scheduler}: {e}"))
        return EXIT_SCHEDULER_ERROR

    if options.list_tests:
        return run_pytest(cfg, scheduler)

    try:
        logging.info('running all tests')
        # Run pytest in the default thread pool executor so the event loop stays
        # responsive (e.g. signal handlers continue to work while pytest runs).
        loop = asyncio.get_running_loop()
        exit_code = await loop.run_in_executor(None, run_pytest, cfg, scheduler)
        logging.info('after running all tests')
    except asyncio.CancelledError:
        print('\ntests cancelled by signal')
        return 1
    except Exception as e:
        print(palette.fail(e))
        raise
    if exit_code == 5:
        print(palette.fail("No tests were collected. Please check the test names and modes you provided, as well as"
                           "the test markers if you used the '--markers' option."
                           "Alternatively you can check with --list option if there any errors."))
    if 'coverage' in options.modes:
        coverage.generate_coverage_report(
            str(coverage_utils.coverage_dir(pathlib.Path(options.tmpdir) / "coverage")),
            path_to("coverage", "test"),
        )

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
    sources_to_exclude = [line for line in open("coverage_excludes.txt", 'r').read().split('\n') if line and not line.startswith('#')]

    # The retired TestSuite registry used to hand out the suites that ran; the
    # pytest runner instead writes per-suite raw profiles to
    # `<tmpdir>/<mode>/coverage/<suite>/*.profraw`, so discover them from disk.
    ran_suites = []
    for mode in modes_for_coverage:
        coverage_root = coverage_utils.coverage_dir(pathlib.Path(options.tmpdir) / mode)
        if not coverage_root.is_dir():
            continue
        for suite_dir in sorted(p for p in coverage_root.iterdir() if p.is_dir()):
            ran_suites.append((suite_dir.name, mode, suite_dir))

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

    for name, mode, coverage_path in ran_suites:
        # 1. Transform every suite raw profiles into indexed profiles
        raw_profiles = list(coverage_path.glob("*.profraw"))
        if len(raw_profiles) == 0:
            logger.warning(f"Couldn't find any raw profiles for suite '{name}' in mode '{mode}' ({coverage_path}):\n\t"
                "1. The binaries are killed instead of terminating which bypasses profile dump.\n\t"
                "2. The suite tempres with the LLVM_PROFILE_FILE which causes the profile to be dumped\n\t"
                "   to somewhere else.")
            continue
        mode_stats = stats.get_node(mode)
        if not mode_stats:
            mode_stats = stats.create_node(tag = time.time(),
                                           identifier = mode,
                                           parent = ROOT_NODE,
                                           data = Stats(f"{mode} mode processing stats", 0, 0))

        raw_stats_node = stats.get_node(mode_stats.identifier + RAW_PROFILE_STATS)
        if not raw_stats_node:
            raw_stats_node = stats.create_node(tag = time.time(),
                                               identifier = mode_stats.identifier + RAW_PROFILE_STATS,
                                               parent = mode_stats,
                                               data = Stats(RAW_PROFILE_STATS, 0, 0))
        stat = stats.create_node(tag = time.time(),
                                 identifier = raw_stats_node.identifier + name,
                                 parent = raw_stats_node,
                                 data = Stats(name, pathsize(coverage_path), 0))
        raw_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, raw_stats_node.data.size)


        logger.info(f"{name}: Converting raw profiles into indexed profiles - {stat.data}.")
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
                                 identifier = indexed_stats_node.identifier + name,
                                 parent = indexed_stats_node,
                                 data = Stats(name, pathsize(coverage_path), time.time() - start_time))
        indexed_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, indexed_stats_node.data.size)

        logger.info(f"{name}: Done converting raw profiles into indexed profiles - {humanfriendly.format_timespan(stat.data.time)}.")

        # 2. Transform every indexed profile into an lcov trace file,
        #    after this step, the dependency upon the build artifacts
        #    ends and processing of the files can be done using the source
        #    code only.

        logger.info(f"{name}: Converting indexed profiles into lcov trace files.")
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
                                 identifier = lcov_conversion_stats_node.identifier + name,
                                 parent = lcov_conversion_stats_node,
                                 data = Stats(name, pathsize(coverage_path), time.time() - start_time))
        lcov_conversion_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, lcov_conversion_stats_node.data.size)

        logger.info(f"{name}: Done converting indexed profiles into lcov trace files - {humanfriendly.format_timespan(stat.data.time)}.")

        # 3. combine all tracefiles
        logger.info(f"{name} in mode {mode}: Combinig lcov trace files.")
        start_time = time.time()
        trace_files = list(coverage_path.glob("**/*.info"))
        target_trace_file = coverage_path / (name + ".info")
        if len(trace_files) == 0: # No coverage data, can skip
            logger.warning(f"{name} in mode  {mode}: No coverage tracefiles found")
            continue
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
                                 identifier = lcov_merge_stats_node.identifier + name,
                                 parent = lcov_merge_stats_node,
                                 data = Stats(name, pathsize(coverage_path), time.time() - start_time))
        lcov_merge_stats_node.data += stat.data
        mode_stats.data.time += stat.data.time
        mode_stats.data.size = max(mode_stats.data.size, lcov_merge_stats_node.data.size)

        suits_trace_files.setdefault(mode, {})[name] = target_trace_file
        logger.info(f"{name}: Done combinig lcov trace files - {humanfriendly.format_timespan(stat.data.time)}")

    #4. combine the suite lcovs into per mode trace files
    modes_trace_files  = {}
    for mode, suite_traces in suits_trace_files.items():

        target_trace_file = coverage_utils.coverage_dir(pathlib.Path(options.tmpdir) / mode) / f"{mode}_coverage.info"
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

    if sys.version_info < (3, 14):
        print("Python 3.14 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
