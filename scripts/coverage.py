#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 ScyllaDB
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
import inspect
import os.path
import subprocess
import sys


def __raw_profiling_filename(test_path):
    return test_path + ".profraw"


def env(test_path):
    """Generate the env variables required by the test

    Such that the generated profiling data fulfills generate_coverage_report()'s
    requirements.
    """
    return {"LLVM_PROFILE_FILE": __raw_profiling_filename(test_path)}


def run(args):
    """Run the command, setting the required env variables

    In order to generate the profiling data in the right place. See env().
    """
    subprocess.check_call(args, env=dict(os.environ, **env(args[0])))


def generate_coverage_report(path="build/coverage/test", name="tests", input_files=None, verbose=0):
    """Generate a html coverage report from the given profiling data

    Arguments:
    * PATH - path to the directory where the raw profiling output as well as
      the test executables producing them can be found. Raw profiling output will
      be automatically picked up and merged to provide the combined output.
      Profiling output files are expected to have names that matches that of the
      test executable that generated them, plus the compiler specific extension.
      E.g. for clang and a test executable named 'querier_cache_test', the
      matching profiling output should be 'querier_cache_test.profraw'.
      For clang, this can be achieved by setting the LLVM_PROFILE_FILE env
      variable to the appropriate value when running the test. This is
      automatically done by test.py.
      The PATH is typically 'build/coverage/test' when the script is ran from
      the scylla repository root.
    * NAME - the name of the generated report. This will be the name of the
      directory containing the generated html report, as well as the name of any
      intermediate files generated in the process (with the appropriate
      extensions).
    * INPUT_FILES (optional) - the list of raw profiling data to generate the
      report from. When provided, this overrides the automatic search for
      profiling data found in PATH and the profiling report will only include
      the files provided herein.
      If not provided, the input files are located with the automatic search
      described in PATH instead.
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
    profraw_extension = ".profraw"
    profraw_extension_len = len(profraw_extension)
    test_executables = []

    def maybe_print(msg):
        if verbose:
            print(msg)

    if input_files:
        maybe_print(f"Using input_files as input for the report")
        profraw_files = input_files
        for file in profraw_files:
            dirname, basename = os.path.split(file)
            test_executables.append(os.path.join(dirname, basename[:-profraw_extension_len]))
    else:
        maybe_print(f"Scanning {path} for input files matching *.{profraw_extension}")
        profraw_files = []
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(profraw_extension):
                    profraw_files.append(os.path.join(root, file))
                    test_executables.append(os.path.join(root, file[:-profraw_extension_len]))
        maybe_print(f"Found {len(profraw_files)} input files")

    profdata_path = os.path.join(path, f"{name}.profdata")

    maybe_print(f"Merging raw profiling data {profraw_files}")

    subprocess.check_call(['llvm-profdata', 'merge', '-sparse', f'-o={profdata_path}'] + profraw_files)

    maybe_print(f"Profiling data merged to {profdata_path}")

    info_path = os.path.join(path, f"{name}.info")

    with open(info_path, "w") as f:
        maybe_print(f"Exporting in lcov format to {info_path}")
        subprocess.check_call(["llvm-cov", "export", "-format=lcov", f"-instr-profile={profdata_path}"] + [f"-object={exe}" for exe in test_executables], stdout=f)

    html_report_path = os.path.join(path, f"{name}")
    html_report_url = os.path.abspath(os.path.join(html_report_path, "index.html"))

    maybe_print(f"Generating html report in {html_report_path}")
    if verbose > 1:
        genhtml_cmd = ["genhtml"]
    else:
        genhtml_cmd = ["genhtml", "-q"]
    subprocess.check_call(genhtml_cmd + ["-o", html_report_path, info_path])

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
        $ ./coverage.py --no-coverage-report --run /path/to/my_test1 --testarg1 --testarg2 ...
        $ ./coverage.py --input-files /path/to/my_test1.profraw /path/to/my_test2.profraw

      Alternatively, you can run `./coverage.py` without args to generate a report from all input files it can find.

    Note that `--path`, `--name` and `--verbose` can always be provided.
    """

    class Value(argparse.Action):
        def __init__(self, val, is_default=False):
            self.val = val
            self.is_default = is_default

    arg_parser = argparse.ArgumentParser(description=inspect.getdoc(generate_coverage_report), formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=inspect.getdoc(main))

    arg_parser.add_argument("--path", dest="path", action="store", type=str, required=False, default="build/coverage/test", help="defaults to 'build/coverage/test'")
    arg_parser.add_argument("--name", dest="name", action="store", type=Value, required=False, default=Value("tests", is_default=True), help="defaults to 'tests', with --run it defaults to the name of the provided executable")
    arg_parser.add_argument("--input-files", dest="input_files", nargs='+', action="extend", type=str, required=False)
    arg_parser.add_argument("--verbose", "-v", dest="verbose", action="count", required=False, default=0, help="defaults to not verbose")
    arg_parser.add_argument("--run", dest="run", action="store_true", required=False,
            help="run the specified executable and generate the coverage report, all command line arguments after --run are considered to be part of the to-be-run test")
    arg_parser.add_argument("--no-coverage-report", dest="no_coverage_report", action="store_true", required=False, default=False,
            help="modifier for --run: don't generate a coverage report after running the executable, ignored when --run is not used")

    if '--run' in argv:
        pos = argv.index('--run')
        argv_head = argv[1:pos + 1]
        argv_tail = argv[pos + 1:]
    else:
        argv_head = argv[1:]
        argv_tail = []

    args = arg_parser.parse_args(argv_head)

    if args.run:
        run(argv_tail)
        if args.name.is_default:
            args.name.val = os.path.basename(argv_tail[0])
        input_files = [__raw_profiling_filename(argv_tail[0])]
    else:
        input_files = args.input_files

    if args.no_coverage_report:
        if args.run:
            return
        else:
            print("Ignoring --no-coverage-report as --run was not provided")

    generate_coverage_report(args.path, args.name.val, input_files, args.verbose)


if __name__ == "__main__":
    main(sys.argv)
