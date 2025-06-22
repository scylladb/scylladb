#
# Copyright (c) 2014 Bruno Oliveira
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# 
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import logging
import os
from abc import ABC
from pathlib import Path
from typing import Sequence

from pytest import Config

from test import TOP_SRC_DIR
from test.pylib.cpp.util import make_test_object
from test.pylib.resource_gather import get_resource_gather

TIMEOUT_DEBUG = 60 * 30 # seconds
TIMEOUT = 60 * 15 # seconds

logger = logging.getLogger(__name__)

class CppTestFailure(Exception):
    def __init__(self, filename: str, line_num: int, contents: str) -> None:
        self.filename = filename
        self.line_num = line_num
        self.lines = contents.splitlines()

    def get_lines(self) -> list[tuple[str, tuple[str, ...]]]:
        m = ("red", "bold")
        return [(x, m) for x in self.lines]

    def get_file_reference(self) -> tuple[str, int]:
        return self.filename, self.line_num

class CppTestFailureList(Exception):
    def __init__(self, failures: Sequence[CppTestFailure]) -> None:
        self.failures = list(failures)

class CppTestFacade(ABC):
    def __init__(self, config: Config, combined_tests: dict[str, list[str]] = None):
        self.temp_dir: Path = Path(config.getoption('tmpdir'))
        self.run_id: int = config.getoption('run_id') or 1
        self.gather_metrics: bool = config.getoption('gather_metrics')
        self.save_log_on_success: bool = config.getoption('save_log_on_success')
        self.combined_suites: dict[str, list[str]] = combined_tests

    def list_tests(self, executable: Path , no_parallel: bool, mode: str) -> tuple[bool,list[str]]:
        raise NotImplementedError

    def run_test(self, executable: Path, original_name: str, test_id: str, mode: str, file_name: Path,
                 test_args: Sequence[str] = (), env: dict = None) -> tuple[Sequence[CppTestFailure] | None, str]:
         raise NotImplementedError

    def run_process(self, test_name: str, mode: str, file_name: Path, args: list[str] = (),
                    env: dict = None) -> \
            tuple[bool, Path, int]:
        root_log_dir = self.temp_dir / mode
        stdout_file_path = root_log_dir / f"{test_name}_stdout.log"
        test = make_test_object(test_name, file_name.parent.name, self.run_id, mode, log_dir=self.temp_dir)

        resource_gather = get_resource_gather(self.gather_metrics, test=test)
        resource_gather.make_cgroup()
        os.chdir(TOP_SRC_DIR)
        timeout = TIMEOUT_DEBUG if mode == 'debug' else TIMEOUT
        p, out = resource_gather.run_process(args, timeout, env)

        with open(stdout_file_path, 'w') as fd:
            fd.write(out)
        metrics = resource_gather.get_test_metrics()
        test_passed = p.returncode == 0
        resource_gather.write_metrics_to_db(metrics, success=test_passed)
        resource_gather.remove_cgroup()
        return test_passed, stdout_file_path, p.returncode
