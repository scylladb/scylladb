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
                'working_dir: {working_dir}\n'
                'Internal Error: calling {executable} '
                'for test {test_id} failed (returncode={returncode}):\n'
                'output:{stdout}\n'
                'command to repeat:{command}'
            )
            failure = CppTestFailure(
                file_name.name,
                line_num=0,
                contents=msg.format(
                    working_dir=os.getcwd(),
                    executable=executable,
                    test_id=test_name,
                    stdout=stdout_file_path.absolute(),
                    command=' '.join(args),
                    returncode=return_code,
                ),
            )
            return [failure], ''
        return None, ''
