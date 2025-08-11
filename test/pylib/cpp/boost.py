#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
import logging
import subprocess
from functools import cache, cached_property
from itertools import chain
from textwrap import dedent
from typing import TYPE_CHECKING
from xml.etree import ElementTree

import allure

from test.pylib.cpp.base import CppFile, CppTestFailure

if TYPE_CHECKING:
    import pathlib

    from test.pylib.cpp.base import CppTestCase


COMBINED_TESTS = "combined_tests"

logger = logging.getLogger(__name__)


class BoostTestFile(CppFile):
    @cached_property
    def exe_path(self) -> pathlib.Path:
        path = super().exe_path
        if not path.is_file():
            path = self.build_basedir / COMBINED_TESTS
            if not path.is_file() or self.test_name not in get_boost_test_list_content(executable=path, combined=True):
                raise FileNotFoundError(
                    f"There is no separate {self.build_mode} binary built for {self.path.name},"
                    " and it's not built into the combined tests binary",
                )
        return path

    @cached_property
    def combined(self) -> bool:
        return self.exe_path.name == COMBINED_TESTS

    @cached_property
    def no_parallel(self) -> bool:
        """Run all test cases in a single process."""

        return self.test_name in self.suite_config.get("no_parallel_cases", [])

    def list_test_cases(self) -> list[str]:
        if self.no_parallel:
            return [self.test_name]
        return get_boost_test_list_content(executable=self.exe_path, combined=self.combined)[self.test_name]

    def run_test_case(self, test_case: CppTestCase) -> tuple[None | list[CppTestFailure], str]:
        run_test = f"{self.test_name}/{test_case.test_case_name}" if self.combined else test_case.test_case_name

        log_sink = test_case.get_artifact_path(suffix=".xml")
        args = [
            "--report_level=no",
            "--output_format=XML",
            f"--log_sink={log_sink}",
            "--catch_system_errors=no",
            "--color_output=false",
        ]
        if not self.no_parallel:
            args.append(f"--{run_test=}")

        args.append("--")

        if random_seed := self.config.getoption("--random-seed"):
            args.append(f"--random-seed={random_seed}")
        args.extend(self.test_args)

        stdout_file_path = test_case.get_artifact_path(extra="_stdout", suffix=".log").absolute()
        process = test_case.run_exe(test_args=args, output_file=stdout_file_path)

        try:
            log_xml = log_sink.read_text(encoding="utf-8")
        except IOError:
            log_xml = ""
        results = parse_boost_test_log_sink(log_xml=log_xml)

        if return_code := process.returncode:
            allure.attach(stdout_file_path.read_bytes(), name="output", attachment_type=allure.attachment_type.TEXT)
            return [CppTestFailure(
                file_name=self.path.name,
                line_num=results[0].line_num if results else -1,
                content=dedent(f"""\
                    working_dir: {os.getcwd()}
                    Internal Error: calling {self.exe_path} for test {run_test} failed ({return_code=}):
                    output file: {stdout_file_path}
                    log: {log_xml}
                    command to repeat: {subprocess.list2cmdline(process.args)}
                """),
            )], ""

        if not self.config.getoption("--save-log-on-success"):
            log_sink.unlink(missing_ok=True)
            stdout_file_path.unlink(missing_ok=True)

        return None, ""


pytest_collect_file = BoostTestFile.pytest_collect_file


@cache
def get_boost_test_list_content(executable: pathlib.Path, combined: bool = False) -> dict[str, list[str]]:
    """List the content of test tree in an executable.

    Return a dict where key is the name of test file and value is a list of tests in this file.  In case of
    combined tests the dict will have multiple items, otherwise we assume that name of the executable is the same
    as the source test file (.cc)

    --list_content produces the list of all test cases in the file.  When BOOST_DATA_TEST_CASE is used it
    additionally produce the lines with numbers for each case preserving the function name like this:

    test_singular_tree_ptr_sz*
        _0*
        _1*
        _2*

    or, in case of combined tests:

    group0_voter_calculator_test*
        existing_voters_are_kept_across_racks*
        leader_is_retained_as_voter*
            _0*
            _1*
            _2*

    Lines like '_0' are ignored because we count a test with a dataprovider as one test case.
    """
    output = subprocess.check_output(
        [executable, "--list_content"],
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    test_tree = {}
    current_suite = []

    if combined:
        test_indent = "    "
    else:
        test_indent = ""
        test_tree[executable.name] = current_suite

    for line in output.splitlines():
        name = line.strip().removesuffix("*")  # remove spaces around and trailing `*`

        if name.startswith("_"):
            # TODO: add support for test cases with dataprovider
            continue

        if line.startswith(test_indent):
            current_suite.append(name)
        else:
            current_suite = test_tree[name] = []

    return test_tree


def parse_boost_test_log_sink(log_xml: str) -> list[CppTestFailure]:
    """Parse the output of 'log' section produced by BoostTest.

    This is always an XML file, and from this it's possible to parse most of the failures
    possible when running BoostTest.
    """
    result = []

    try:
        log_root = ElementTree.fromstring(text=log_xml)
        if log_root is not None:
            for elem in chain.from_iterable(log_root.findall(tag) for tag in ("Exception", "Error", "FatalError")):
                last_checkpoint = elem.find("LastCheckpoint")
                if last_checkpoint is not None:
                    elem = last_checkpoint
                result.append(CppTestFailure(
                    file_name=elem.attrib["file"],
                    line_num=int(elem.attrib["line"]),
                    content=elem.text or "",
                ))
    except ElementTree.ParseError:
        logger.warning("Error parsing the log_sink output. Can be empty or invalid.")

    return result
