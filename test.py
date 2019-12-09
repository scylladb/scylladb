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
import argparse
import asyncio
import glob
import io
import logging
import multiprocessing
import os
import pathlib
import signal
import subprocess
import sys
import xml.etree.ElementTree as ET

# Apply custom options to these tests
custom_test_args = {
    'boost/mutation_reader_test': '-c{} -m2G'.format(min(os.cpu_count(), 3)),
    'boost/sstable_test': '-c1 -m2G',
    'boost/sstable_datafile_test': '-c1 -m2G',
    'boost/sstable_3_x_test': '-c1 -m2G',
    'unit/lsa_async_eviction_test': '-c1 -m200M --size 1024 --batch 3000 --count 2000000',
    'unit/lsa_sync_eviction_test': [
        '-c1 -m100M --count 10 --standard-object-size 3000000',
        '-c1 -m100M --count 24000 --standard-object-size 2048',
        '-c1 -m1G --count 4000000 --standard-object-size 128'
        ],
    'unit/row_cache_alloc_stress_test': '-c1 -m2G',
    'unit/row_cache_stress_test': '-c1 -m1G --seconds 10',
}

# Only run in dev, release configurations, skip in others
long_tests = set([
    'unit/lsa_async_eviction_test',
    'unit/lsa_sync_eviction_test',
    'unit/row_cache_alloc_stress_test',
    'unit/row_cache_stress_test'
])

CONCOLORS = {'green': '\033[1;32m', 'red': '\033[1;31m', 'nocolor': '\033[0m'}

def colorformat(msg, **kwargs):
    fmt = dict(CONCOLORS)
    fmt.update(kwargs)
    return msg.format(**fmt)


def status_to_string(success):
    if success:
        status = colorformat("{green}[ PASS ]{nocolor}") if os.isatty(sys.stdout.fileno()) else "[ PASS ]"
    else:
        status = colorformat("{red}[ FAIL ]{nocolor}") if os.isatty(sys.stdout.fileno()) else "[ FAIL ]"

    return status


class UnitTest:
    standard_args = '--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000 --collectd 0'.split()
    seastar_args = '-c2 -m2G'

    def __init__(self, test_no, name, opts, kind, mode, options):
        if opts is None:
            opts = UnitTest.seastar_args
        self.id = test_no
        # Name with test suite name
        self.name = name
        # Name within the suite
        self.shortname = os.path.basename(name)
        self.mode = mode
        self.kind = kind
        self.path = os.path.join("build", self.mode, "test", self.name)
        self.args = opts.split() + UnitTest.standard_args
        # Unique file name, which is also readable by human, as filename prefix
        self.uname = "{}.{}.{}".format(self.mode, self.shortname, self.id)
        self.log_filename = os.path.join(options.tmpdir, self.uname + ".log")

        if self.kind == 'boost':
            boost_args = []
            xmlout = os.path.join(options.jenkins, self.uname + ".boost.xml")
            boost_args += ['--report_level=no', '--logger=HRF,test_suite:XML,test_suite,' + xmlout]
            boost_args += ['--']
            self.args = boost_args + self.args


def print_start_blurb():
    print("="*80)
    print("{:7s} {:50s} {:^8s} {:8s}".format("[N/TOTAL]", "TEST", "MODE", "RESULT"))
    print("-"*78)


def print_end_blurb(verbose):
    if not verbose:
        sys.stdout.write('\n')
    print("-"*78)


def print_progress(test, success, cookie, verbose):
    if isinstance(cookie, int):
        cookie = (0, 1, cookie)

    last_len, n, n_total = cookie
    msg = "{:9s} {:50s} {:^8s} {:8s}".format(
        "[{}/{}]".format(n, n_total),
        test.name, test.mode[:8],
        status_to_string(success)
    )
    if verbose is False:
        print('\r' + ' ' * last_len, end='')
        last_len = len(msg)
        print('\r' + msg, end='')
    else:
        print(msg)

    return (last_len, n + 1, n_total)


async def run_test(test, options):
    file = io.StringIO()

    def report_error(out):
        print('=== stdout START ===', file=file)
        print(out, file=file)
        print('=== stdout END ===', file=file)
    success = False
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
        success = process.returncode == 0
        if process.returncode != 0:
            print('  with error code {code}\n'.format(code=process.returncode), file=file)
            report_error(stdout.decode(encoding='UTF-8'))

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
    logging.info("Test #%d %s", test.id, "passed" if success else "failed")
    return (test, success)

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
    parser.add_argument(
        "--jenkins",
        action="store",
        help="""Jenkins output file prefix. Default: ${tmpdir}/xml"""
    )
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument(
        "--xunit",
        action="store",
        help="""Name of a file to write results of non-boost tests to in
        xunit format. Default: ${tmpdir}/xml/xunit.xml"""
    )
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
    if not args.jenkins or not args.xunit:
        xmldir = os.path.join(args.tmpdir, "xml")
        prepare_dir(xmldir, "*.xml")
        if args.jenkins is None:
            args.jenkins = xmldir
        if args.xunit is None:
            args.xunit = os.path.join(xmldir, "xunit.xml")

    return args


def find_tests(options):

    tests_to_run = []

    def add_test_list(kind, mode):
        lst = glob.glob(os.path.join("test", kind, "*_test.cc"))
        for t in lst:
            t = os.path.join(kind, os.path.splitext(os.path.basename(t))[0])
            if mode not in ["release", "dev"] and t in long_tests:
                continue
            args = custom_test_args.get(t)
            if isinstance(args, (str, type(None))):
                args = [args]
            for a in args:
                patterns = options.name if options.name else [t]
                for p in patterns:
                    if p in t:
                        tests_to_run.append((t, a, kind, mode))

    for mode in options.modes:
        add_test_list('unit', mode)
        add_test_list('boost', mode)

    if not tests_to_run:
        print("Test {} not found".format(options.name))
        sys.exit(1)

    logging.info("Found %d tests, repeat count is %d, starting %d concurrent jobs",
                 len(tests_to_run), options.repeat, options.jobs)

    tests_to_run = [t for t in tests_to_run for _ in range(options.repeat)]
    tests_to_run = [UnitTest(test_no, *t, options) for test_no, t in enumerate(tests_to_run)]

    return tests_to_run


async def run_all_tests(tests_to_run, signaled, options):
    failed_tests = []
    results = []
    cookie = len(tests_to_run)
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
            results.append(result)
            test, success = result
            cookie = print_progress(test, success, cookie, options.verbose)
            if not success:
                failed_tests.append(test)
    print_start_blurb()
    try:
        for test in tests_to_run:
            # +1 for 'signaled' event
            if len(pending) > options.jobs:
                # Wait for some task to finish
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                await reap(done, pending, signaled)
            pending.add(asyncio.create_task(run_test(test, options)))
        # Wait & reap ALL tasks but signaled_task
        # Do not use asyncio.ALL_COMPLETED to print a nice progress report
        while len(pending) > 1:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            await reap(done, pending, signaled)

    except asyncio.CancelledError:
        return None, None

    print_end_blurb(options.verbose)

    return failed_tests, results


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


def print_summary(failed_tests, total_tests):
    if failed_tests:
        print('\n\nOutput of the failed tests:')
        for test in failed_tests:
            print("Test {} {} failed:".format(test.path, " ".join(test.args)))
            print(read_log(test.log_filename))
        print('\n\nThe following test(s) have failed:')
        for test in failed_tests:
            print('  {} {}'.format(test.path, ' '.join(test.args)))
        print('\nSummary: {} of the total {} tests failed'.format(len(failed_tests), total_tests))


def write_xunit_report(options, results):
    unit_results = [r for r in results if r[0].kind != 'boost']
    num_unit_failed = sum(1 for r in unit_results if not r[1])

    xml_results = ET.Element('testsuite', name='non-boost tests',
            tests=str(len(unit_results)), failures=str(num_unit_failed), errors='0')

    for test, success in unit_results:
        xml_res = ET.SubElement(xml_results, 'testcase', name=test.path)
        if not success:
            xml_fail = ET.SubElement(xml_res, 'failure')
            xml_fail.text = "Test {} {} failed:".format(test.path, " ".join(test.args))
            xml_fail.text += read_log(test.log_filename)
    with open(options.xunit, "w") as f:
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

    tests_to_run = find_tests(options)
    signaled = asyncio.Event()

    setup_signal_handlers(asyncio.get_event_loop(), signaled)

    failed_tests, results = await run_all_tests(tests_to_run, signaled, options)

    if signaled.is_set():
        return -signaled.signo

    print_summary(failed_tests, len(tests_to_run))

    write_xunit_report(options, results)

    return 0 if not failed_tests else -1

if __name__ == "__main__":
    if sys.version_info < (3, 7):
        print("Python 3.7 or newer is required to run this program")
        sys.exit(-1)
    sys.exit(asyncio.run(main()))
