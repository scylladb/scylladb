#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

from test import TOP_SRC_DIR
from test.pylib.cpp.facade import CppTestFacade, CppTestFailure, run_process

TIMEOUT = 60 * 10 # seconds

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
        os.chdir(TOP_SRC_DIR)
        p, stdout = run_process(args, TIMEOUT)

        if p.returncode != 0:
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
                    stdout=stdout,
                    command=' '.join(p.args),
                    returncode=p.returncode,
                ),
            )
            return [failure], stdout
        return None, stdout
