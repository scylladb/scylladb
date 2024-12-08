#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations

import os
from typing import Sequence

from test.pylib.cpp.facade import CppTestFacade, CppTestFailure, run_process

TIMEOUT = 30 # seconds

class UnitTestFacade(CppTestFacade):

    @staticmethod
    def list_tests(
            executable: str,
            no_parallel_run: bool
    ) -> list[str]:
        return [os.path.basename(os.path.splitext(executable)[0])]

    def run_test(
        self,
        executable: str,
        mode: str,
        test_id: str,
        file_name: str,
        test_args: Sequence[str] = (),
    ) -> tuple[Sequence[CppTestFailure] | None, str]:
        args = [executable, *test_args]
        os.chdir(self.temp_dir.parent)
        p, stderr, stdout = run_process(args, TIMEOUT)

        if p.returncode != 0:
            msg = (
                'working_dir: {working_dir}\n'
                'Internal Error: calling {executable} '
                'for test {test_id} failed (returncode={returncode}):\n'
                'output:{stdout}\n'
                'std error:{stderr}\n'
                'command to repeat:{command}'
            )
            failure = CppTestFailure(
                file_name,
                line_num=0,
                contents=msg.format(
                    working_dir=os.getcwd(),
                    executable=executable,
                    test_id=test_id,
                    stdout=stdout,
                    stderr=stderr,
                    command=' '.join(p.args),
                    returncode=p.returncode,
                ),
            )
            return [failure], stdout
        return None, stdout
