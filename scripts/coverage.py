#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


import argparse
import inspect
import os.path
import subprocess
import sys
import re


__DISTINCT_ID_RE = "[-_a-z0-9]+"


def __validate_distinct_id(distinct_id):
    if not re.fullmatch(__DISTINCT_ID_RE, str(distinct_id)):
        raise ValueError(f"Invalid distinct_id: {distinct_id}, a valid id contains only letters, numbers and '_' characters")


def __raw_profiling_filename(test_path, distinct_id=None):
    if distinct_id:
        __validate_distinct_id(distinct_id)
        return f"{test_path}.profraw.{distinct_id}"
    else:
        return f"{test_path}.profraw"


def env(test_path, distinct_id=None):
    """Generate the env variables required by the test

    Such that the generated profiling data fulfills generate_coverage_report()'s
    requirements.
    If the executable at `test_path` is not unique in a test run, meaning that
    it is ran multiple times as part of different test suites, or test cases of
    the same suite, `distinct_id` can be used to distinguish between these,
    ensuring that the coverage report will include all these different runs,
    instead of whatever happened to run last time (overriding previous results).
    """
    return {"LLVM_PROFILE_FILE": __raw_profiling_filename(test_path, distinct_id)}


def run(args, executable=None, distinct_id=None):
    """Run the command, setting the required env variables

    In order to generate the profiling data in the right place. See env().
    The `executable` can be used to override the executable used to setup the
    env. This is useful for tests ran via a script, where args[0] is not the
    executable itself, but said script. By default `args[0]` is used.
    To distinguish between multiple runs of the same executable, use
    `distinct_id`, see `env()` for more details.
    """
    if executable:
        extra_env = env(executable, distinct_id)
    else:
        extra_env = env(args[0], distinct_id)
    try:
        subprocess.check_call(args, env=dict(os.environ, **extra_env))
    except KeyboardInterrupt:
        pass # allow process to be shut down with ^C


def generate_coverage_report(path="testlog/coverage/coverage", build_path="build/coverage/test", name="tests", input_files=None, verbose=0):
    """Generate a html coverage report from the given profiling data

    Arguments:
    * PATH - path to the directory where test.py collects raw profiling
      output for a test run, and where the merged output (profdata, lcov and
      html report) will be written. This mirrors the layout test.py already
      uses for a Scylla cluster's own coverage
      (`<tmpdir>/<mode>/coverage/<suite>/`): raw profiles for the unit-test
      suites under BUILD_PATH are expected at PATH/<suite>/*.profraw.
    * BUILD_PATH - path to the directory containing the test executables, one
      subdirectory per suite (e.g. 'boost', 'raft', 'ldap', 'unit'). Used only
      by the automatic search (see INPUT_FILES below) to enumerate suites and
      map a raw profile back to the executable that produced it.
      The BUILD_PATH is typically 'build/coverage/test' when the script is
      ran from the scylla repository root.
    * NAME - the name of the generated report. This will be the name of the
      directory containing the generated html report, as well as the name of any
      intermediate files generated in the process (with the appropriate
      extensions).
    * INPUT_FILES (optional) - the list of raw profiling data to generate the
      report from. When provided, this overrides the automatic search for
      profiling data found under PATH/BUILD_PATH and the profiling report
      will only include the files provided herein. Each input file's
      executable is inferred from its own name, matching the raw profiling
      data's name minus its extension (e.g. 'querier_cache_test.profraw' next
      to executable 'querier_cache_test').
      If not provided, the input files are located with the automatic search
      described above instead.
      Note that even if provided, PATH is still used to store intermediate
      files, as well as the final result.
    * VERBOSE (optional) - set verbosity level:
        - 0 (False): no messages, except the one with the path to the generated
          report;
        - 1 (True): print a message at each stage of the report generation;
        - 2: make subcommands verbose (those that support it);

        Defaults to 0 (False).
    """
    verbose = int(verbose)
    input_file_re_str = rf"(.+)\.profraw(\.{__DISTINCT_ID_RE})?"
    input_file_re = re.compile(input_file_re_str)
    test_executables = []

    def maybe_print(msg):
        if verbose:
            print(msg)

    if input_files:
        maybe_print("Using input_files as input for the report")
        profraw_files = input_files
        for file in profraw_files:
            dirname, basename = os.path.split(file)
            match = re.fullmatch(input_file_re, basename)
            if match is None:
                print(f"Error: input file {basename} doesn't match the expected input file naming pattern {input_file_re_str}, skipping it")

            test_executables.append(os.path.join(dirname, match.group(1)))
    else:
        maybe_print(f"Scanning {path} for input files matching {input_file_re_str}, using executables from {build_path}")
        profraw_files = []
        # Cluster-based suites (topology, cql, alternator, ...) exercise the
        # scylla server binary itself rather than a per-suite binary under
        # build_path, and name their raw profiles after the LLVM %m pool
        # token (see runner.py:create_cluster_factory), not a test name.
        scylla_exe = os.path.join(os.path.dirname(os.path.normpath(build_path)), "scylla")
        # Walk the raw-profile tree itself (one subdirectory per suite that
        # actually ran), not build_path: a suite with no dedicated binary
        # directory there (e.g. cql, cqlpy, alternator, cluster, rest_api)
        # would otherwise be skipped entirely, silently dropping most of the
        # project's coverage.
        suite_dirs = sorted(os.scandir(path), key=lambda e: e.name) if os.path.isdir(path) else []
        for suite_dir in suite_dirs:
            if not suite_dir.is_dir():
                continue
            build_suite_dir = os.path.join(build_path, suite_dir.name)
            for root, dirs, files in os.walk(suite_dir.path):
                for file in files:
                    match = re.fullmatch(input_file_re, file)
                    if match is None:
                        continue
                    profraw_files.append(os.path.join(root, file))
                    if os.path.isdir(build_suite_dir):
                        # unit-test raw profiles are named
                        # `<test_name>.<case>.<run_id>.profraw` (see
                        # test/pylib/cpp/base.py); the executable lives under
                        # build_path, not next to the profile itself. Some
                        # boost tests have no binary of their own and are
                        # built into a suite-wide `combined_tests` executable
                        # instead (see COMBINED_TESTS in
                        # test/pylib/cpp/boost.py), so fall back to that when
                        # <test_name> doesn't exist as its own executable.
                        test_name = match.group(1).split(".")[0]
                        exe_path = os.path.join(build_suite_dir, test_name)
                        if not os.path.isfile(exe_path):
                            exe_path = os.path.join(build_suite_dir, "combined_tests")
                    else:
                        exe_path = scylla_exe
                    test_executables.append(exe_path)
        maybe_print(f"Found {len(profraw_files)} input files")

    if not profraw_files:
        sys.exit("Error: couldn't find any raw profiling data files, can't generate coverage report")

    test_executables = list(dict.fromkeys(test_executables))  # de-dup, preserve order

    os.makedirs(path, exist_ok=True)
    profdata_path = os.path.join(path, f"{name}.profdata")

    maybe_print(f"Merging raw profiling data {profraw_files}")

    subprocess.check_call(['llvm-profdata', 'merge', '-sparse', f'-o={profdata_path}'] + profraw_files)

    maybe_print(f"Profiling data merged to {profdata_path}")

    info_path = os.path.join(path, f"{name}.info")

    with open(info_path, "w") as f:
        maybe_print(f"Exporting in lcov format to {info_path}")
        subprocess.check_call(["llvm-cov", "export", "-format=lcov", f"-instr-profile={profdata_path}"] + [f"-object={exe}" for exe in test_executables], stdout=f)

    html_report_path = os.path.join(path, f"{name}")
    os.makedirs(html_report_path, exist_ok=True)
    html_report_url = os.path.abspath(os.path.join(html_report_path, "index.html"))

    maybe_print(f"Generating html report in {html_report_path}")
    if verbose > 1:
        genhtml_cmd = ["genhtml"]
    else:
        genhtml_cmd = ["genhtml", "-q"]
    # Coverage data merged across independently-built subprojects (e.g. abseil,
    # built as its own CMake subproject) can reference source paths genhtml
    # can't resolve from this checkout; skip annotating those instead of
    # failing the whole report.
    subprocess.check_call(genhtml_cmd + ["--ignore-errors", "source", "--synthesize-missing", "-o", html_report_path, info_path])

    print(f"Coverage report written to {html_report_path}, url: file://{html_report_url}")


def main(argv):
    """This script was intended to support the following use-cases:
    * Generate a report from a recent test run with `test.py`:

        $ ./coverage.py

    * Generate a report from a subset of a recent test run with `test.py`:

        $ ./coverage.py --input-files /path/to/file1 /path/to/file2 ...

    * Run a test directly through `coverage.py` and generate a report immediately:

        $ ./coverage.py --run /path/to/my_test --testarg1 --testarg2 ...

    * Run several tests directly through `coverage.py` and generate a report at the end:

        $ ./coverage.py --no-coverage-report --run /path/to/my_test1 --testarg1 --testarg2 ...
        $ ./coverage.py --no-coverage-report --run /path/to/my_test2 --testarg1 --testarg2 ...
        $ ./coverage.py --input-files /path/to/my_test1.profraw /path/to/my_test2.profraw

      Alternatively, you can run `./coverage.py` without args to generate a report from all input files it can find.

    It is also possible to run tests via a script (`--executable`) or to run the
    same test multiple times, with multiple inputs and generate a final report
    across all runs (`--distinct-id`).

    Note that `--path`, `--name` and `--verbose` can always be provided.
    """

    class Value:
        def __init__(self, val, is_default=False):
            self.val = val
            self.is_default = is_default

    arg_parser = argparse.ArgumentParser(description=inspect.getdoc(generate_coverage_report), formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=inspect.getdoc(main))

    arg_parser.add_argument("--path", dest="path", action="store", type=str, required=False, default="testlog/coverage/coverage", help="defaults to 'testlog/coverage/coverage'")
    arg_parser.add_argument("--build-path", dest="build_path", action="store", type=str, required=False, default="build/coverage/test", help="defaults to 'build/coverage/test'; only used for the automatic search (no --run/--input-files)")
    arg_parser.add_argument("--name", dest="name", action="store", type=Value, required=False, default=Value("tests", is_default=True), help="defaults to 'tests', with --run it defaults to the name of the provided executable")
    arg_parser.add_argument("--input-files", dest="input_files", nargs='+', action="extend", type=str, required=False)
    arg_parser.add_argument("--verbose", "-v", dest="verbose", action="count", required=False, default=0, help="defaults to not verbose")
    arg_parser.add_argument("--run", dest="run", action="store_true", required=False,
            help="run the specified executable and generate the coverage report, all command line arguments after --run are considered to be part of the to-be-run test")
    arg_parser.add_argument("--no-coverage-report", dest="no_coverage_report", action="store_true", required=False, default=False,
            help="modifier for --run: don't generate a coverage report after running the executable, ignored when --run is not used")
    arg_parser.add_argument("--executable", dest="executable", action="store", required=False, default=None,
            help="modifier for --run: the test executable, for tests that are started through a script, ignored when --run is not used")
    arg_parser.add_argument("--distinct-id", dest="distinct_id", action="store", required=False, default=None,
            help="modifier for --run: a distinct id making this run distinct from another one with the same executable, allowing a summary report to be generated across all runs, ignored when --run is not used")

    if '--run' in argv:
        pos = argv.index('--run')
        argv_head = argv[1:pos + 1]
        argv_tail = argv[pos + 1:]
    else:
        argv_head = argv[1:]
        argv_tail = []

    args = arg_parser.parse_args(argv_head)

    if os.path.exists(args.path) and not os.path.isdir(args.path):
        arg_parser.exit(2, f"Error: invalid value for `--path`: '{args.path}' exists and is not a directory\n")

    if args.run:
        run(argv_tail, args.executable, args.distinct_id)
        if args.name.is_default:
            args.name.val = os.path.basename(argv_tail[0])
        if args.executable:
            input_files = [__raw_profiling_filename(args.executable, args.distinct_id)]
        else:
            input_files = [__raw_profiling_filename(argv_tail[0], args.distinct_id)]
    else:
        input_files = args.input_files

    if args.no_coverage_report:
        if args.run:
            return
        else:
            print("Ignoring --no-coverage-report as --run was not provided")

    generate_coverage_report(args.path, args.build_path, args.name.val, input_files, args.verbose)


if __name__ == "__main__":
    main(sys.argv)
