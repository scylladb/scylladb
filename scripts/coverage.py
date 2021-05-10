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


def env(test_path):
    """Generate the env variables required by the test

    Such that the generated profiling data fulfills generate_coverage_report()'s
    requirements.
    """
    return {"LLVM_PROFILE_FILE": test_path + ".profraw"}


def generate_coverage_report(path="build/coverage/test", name="tests"):
    """Generate a html coverage report from all profiling data found at PATH

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
    """
    profraw_files = []
    test_executables = []
    profraw_extension = ".profraw"
    profraw_extension_len = len(profraw_extension)

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(profraw_extension):
                profraw_files.append(os.path.join(root, file))
                test_executables.append(os.path.join(root, file[:-profraw_extension_len]))

    profdata_path = os.path.join(path, f"{name}.profdata")

    subprocess.check_call(['llvm-profdata', 'merge', '-sparse', f'-o={profdata_path}'] + profraw_files)

    info_path = os.path.join(path, f"{name}.info")

    with open(info_path, "w") as f:
        subprocess.check_call(["llvm-cov", "export", "-format=lcov", f"-instr-profile={profdata_path}"] + [f"-object={exe}" for exe in test_executables], stdout=f)

    html_report_path = os.path.join(path, f"{name}")
    html_report_url = os.path.abspath(os.path.join(html_report_path, "index.html"))

    subprocess.check_call(["genhtml", "-q", "-o", html_report_path, info_path])

    print(f"Coverage report written to {html_report_path}, url: file://{html_report_url}")


def main(argv):
    arg_parser = argparse.ArgumentParser(description=inspect.getdoc(generate_coverage_report), formatter_class=argparse.RawDescriptionHelpFormatter)
    arg_parser.add_argument("--path", dest="path", action="store", type=str, required=False, default="build/coverage/test", help="defaults to 'build/coverage/test'")
    arg_parser.add_argument("--name", dest="name", action="store", type=str, required=False, default="tests", help="defaults to 'tests'")
    args = arg_parser.parse_args()

    generate_coverage_report(args.path, args.name)


if __name__ == "__main__":
    main(sys.argv)
