#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
import subprocess
from textwrap import dedent
from typing import TYPE_CHECKING

import allure

from test.pylib.cpp.base import CppFile, CppTestFailure

if TYPE_CHECKING:
    from test.pylib.cpp.base import CppTestCase


class UnitTestFile(CppFile):
    def list_test_cases(self) -> list[str]:
        return [self.test_name]

    def run_test_case(self, test_case: CppTestCase) -> tuple[None | list[CppTestFailure], str]:
        stdout_file_path = test_case.get_artifact_path(extra="_stdout", suffix=".log").absolute()
        process = test_case.run_exe(test_args=self.test_args, output_file=stdout_file_path)

        if return_code := process.returncode:
            allure.attach(stdout_file_path.read_bytes(), name="output", attachment_type=allure.attachment_type.TEXT)
            return [CppTestFailure(
                file_name=self.path.name,
                line_num=0,
                content=dedent(f"""\
                    working_dir: {os.getcwd()}
                    Internal Error: calling {self.exe_path} for test {test_case.test_case_name} failed ({return_code=}):
                    output file: {stdout_file_path}
                    command to repeat: {subprocess.list2cmdline(process.args)}
                """),
            )], ""

        if not self.config.getoption("--save-log-on-success"):
            stdout_file_path.unlink(missing_ok=True)

        return None, ""


pytest_collect_file = UnitTestFile.pytest_collect_file
