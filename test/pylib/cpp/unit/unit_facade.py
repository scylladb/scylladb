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

from test import TOP_SRC_DIR
from test.pylib.cpp.facade import CppTestFacade, CppTestFailure
from test.pylib.cpp.util import make_test_object
from test.pylib.resource_gather import get_resource_gather

TIMEOUT = 30 # seconds

class UnitTestFacade(CppTestFacade):

    def list_tests(
            self,
            executable: Path,
            no_parallel_run: bool
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
        root_log_dir = self.temp_dir / mode
        stdout_file_path = root_log_dir / f"{test_name}_stdout.log"
        test = make_test_object(test_name, file_name.parent.name, mode, original_name)

        resource_gather = get_resource_gather(self.gather_metrics, test=test, tmpdir=str(self.temp_dir))
        resource_gather.make_cgroup()
        args = [str(executable), *test_args]
        os.chdir(TOP_SRC_DIR)

        p, out = resource_gather.run_process(args, TIMEOUT, env)
        with open(stdout_file_path, 'w') as fd:
            fd.write(out)
        metrics = resource_gather.get_test_metrics()
        test_passed = p.returncode == 0
        resource_gather.write_metrics_to_db(metrics, success=test_passed)
        resource_gather.remove_cgroup()

        if not test_passed:
            metrics = resource_gather.get_test_metrics()
            resource_gather.write_metrics_to_db(metrics)
            resource_gather.remove_cgroup()
            allure.attach(out, name='output', attachment_type=allure.attachment_type.TEXT)
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
                    command=' '.join(p.args),
                    returncode=p.returncode,
                ),
            )
            return [failure], out

        return None, out
