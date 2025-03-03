#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Sequence

import allure

from test.pylib.cpp.facade import CppTestFacade, CppTestFailure, run_process
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
        test = make_test_object(test_name, file_name.parent.name, mode, original_name)

        resource_gather = get_resource_gather(self.gather_metrics, test=test, tmpdir=str(self.temp_dir))
        resource_gather.make_cgroup()
        test_running_event = asyncio.Event()
        args = [str(executable), *test_args]
        os.chdir(self.temp_dir.parent)
        test_resource_watcher = resource_gather.cgroup_monitor(test_event=test_running_event)
        test.time_start = time.time()
        p, out = run_process(args, TIMEOUT, env=env, preexec_fn=resource_gather.set_sid(), cgroup=resource_gather.cgroup_path)

        test.time_end = time.time()
        loop = test_resource_watcher.get_loop()
        loop.run_until_complete(asyncio.gather(test_resource_watcher))

        if p.returncode != 0:
            allure.attach(out, name='output', attachment_type=allure.attachment_type.TEXT)
            metrics = resource_gather.get_test_metrics()

            resource_gather.write_metrics_to_db(metrics)
            resource_gather.remove_cgroup()
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
                    stdout=out,
                    command=' '.join(p.args),
                    returncode=p.returncode,
                ),
            )
            return [failure], out

        metrics = resource_gather.get_test_metrics()
        resource_gather.write_metrics_to_db(metrics, success=True)
        resource_gather.remove_cgroup()

        return None, out
