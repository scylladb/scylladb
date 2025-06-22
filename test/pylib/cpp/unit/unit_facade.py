#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

import allure

from test.pylib.cpp.facade import CppTestFacade, CppTestFailure

class UnitTestFacade(CppTestFacade):

    def list_tests(
            self,
            executable: Path,
            no_parallel_run: bool,
            mode: str
    ) -> tuple[bool, list[str]]:
        return False, [os.path.basename(os.path.splitext(executable)[0])]

    def run_test(
        self,
        executable: Path,
        original_name: str,
        test_name: str,
        mode: str,
        file_name: Path,
        test_args: Sequence[str] = (),
        env: dict = None,
    ) -> tuple[list[CppTestFailure], str] | tuple[None, str]:
        args = [str(executable), *test_args]
        test_passed, stdout_file_path, return_code = self.run_process(test_name, mode, file_name, args, env)

        if not test_passed:
            allure.attach(stdout_file_path.read_bytes(), name='output', attachment_type=allure.attachment_type.TEXT)
            msg = (
                f'working_dir: {os.getcwd()}\n'
                f'Internal Error: calling {executable} '
                f'for test {test_name} failed ({return_code=}):\n'
                f'output:{stdout_file_path.absolute()}\n'
                f'command to repeat:{" ".join(args)}'
            )
            failure = CppTestFailure(
                file_name.name,
                line_num=0,
                contents=msg
            )
            return [failure], ''
        if not self.save_log_on_success:
            stdout_file_path.unlink(missing_ok=True)
        return None, ''
